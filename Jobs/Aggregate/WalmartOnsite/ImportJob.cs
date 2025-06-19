using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.Data.DataSource.WalmartOnsite;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.Zip;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.WalmartOnsite;

[Export("WalmartOnsite-AggregateImportJob", typeof(IDragoJob))]
[Export("WalmartOnsite-Aggregate-SearchImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private readonly Stopwatch _runTime = new();

    private RemoteAccessClient _rac;
    private List<OrderedQueue> _queueItems;
    private Uri _baseDestUri;
    private Uri _baseStageDestUri;
    private int _warningCount;
    private int _exceptionCount;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private WalmartOnsiteService _walmartOnsiteService;
    private UnfinishedReportProvider<ApiReportItem> _unfinishedReportProvider;
    private ParallelOptions _apiParallelOptions;

    private TimeSpan _maxRuntime;
    private int _maxAPIRequestPerMinute;
    private int _maxDegreeOfParallelism;


    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _rac = GetS3RemoteAccessClient();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));

        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();
        _baseDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.WALMARTONSITE_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.WALMARTONSITE_MAX_DEGREE_OF_PARALLELISM, 2);
        var maxAPIRequestPerHour = LookupService.GetLookupValueWithDefault(Constants.WALMARTONSITE_MAX_API_REQUESTS_PER_HOUR, 1000);

        _maxAPIRequestPerMinute = maxAPIRequestPerHour / 60;
        _apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism };

        _unfinishedReportProvider = new UnfinishedReportProvider<ApiReportItem>(_baseDestUri, LogMessage, LogException);

        var options = new WalmartOnsiteServiceOptions
        {
            ConsumerId = CurrentCredential.CredentialSet.consumerId,
            KeyVersion = Int32.Parse(CurrentCredential.CredentialSet.key_version),
            PrivateKey = CurrentCredential.CredentialSet.private_key,
            AuthToken = CurrentCredential.CredentialSet.auth_token,
            IntegrationEndpointURI = CurrentIntegration.EndpointURI,
        };

        _walmartOnsiteService = new(options, HttpClientProvider);
    }

    public void Execute()
    {
        _runTime.Start();

        List<ApiReportItem> apiReports = _unfinishedReportProvider.LoadUnfinishedReportsFile(_queueItems);

        if (_queueItems.Count != 0)
        {
            foreach (OrderedQueue queueItem in _queueItems)
            {
                if (HasRuntimeExceeded())
                {
                    break;
                }

                //Documentation states that we cannot retrieve reports for the current date.
                var timeUtc = DateTime.UtcNow;
                TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                if (queueItem.DeliveryFileDate.Equals(easternTime.Date))
                {
                    LogMessage(LogLevel.Warn, $"Cannot import data for current date. Please run queue: {queueItem.ID} tomorrow.");
                    _warningCount++;
                    continue;
                }

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                try
                {
                    DownloadReports(queueItem, apiReports);

                    GenerateSnapshots(queueItem, apiReports);
                    CheckReportStatus(queueItem, apiReports);
                }
                catch (HttpClientProviderRequestException ex)
                {
                    HandleException(queueItem, ex);
                    continue;
                }
                catch (Exception ex)
                {
                    HandleException(queueItem, ex);
                    continue;
                }

                var allReportsDownloaded = apiReports.Where(x => x.QueueID == queueItem.ID).All(x => x.IsDownloaded);
                if (allReportsDownloaded)
                {
                    apiReports.RemoveAll(x => x.QueueID == queueItem.ID);
                    _unfinishedReportProvider.DeleteReport(queueItem.FileGUID.ToString());
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Complete);
                }
                else
                {
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                }
            }
        }
        else
        {
            LogMessage(LogLevel.Info, "There are no reports in the Queue");
        }

        _runTime.Stop();

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
        else if (_warningCount > 0)
        {
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
        }

        LogMessage(LogLevel.Info, "Import job complete");
    }

    private void HandleException<TException>(OrderedQueue queueItem, TException ex) where TException : Exception
    {
        var logMessage = BuildLogErrorMessage(ex);
        LogException(LogLevel.Error, logMessage, ex);
        base.UpdateQueueWithDelete(new List<OrderedQueue> { queueItem }, Common.Constants.JobStatus.Error, false);
        _exceptionCount++;
    }

    private static string BuildLogErrorMessage<TException>(TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"Exception thrown. Exception details : {httpEx}",
            _ => $"Exception thrown. Exception Message: {exception.Message}"
        };
    }

    private void GenerateSnapshots(OrderedQueue queueItem, List<ApiReportItem> apiReportItems)
    {
        foreach (var apiReport in _apiReports)
        {
            if (apiReportItems.Any(x => x.QueueID == queueItem.ID && x.ReportName == apiReport.APIReportName && !x.ShouldGenerateSnapshot))
            {
                continue;
            }

            var apiReportItem = new ApiReportItem
            {
                AdvertiserID = Int32.Parse(queueItem.EntityID),
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportName = apiReport.APIReportName,
                ReportDate = queueItem.FileDate,
                SnapShotEntity = apiReport.ReportSettings.EntityType,
                ReportFields = apiReport.ReportFields,
                ReportType = apiReport.ReportSettings.ReportType,
                FileExtension = apiReport.ReportSettings.FileExtension,
                AttributionWindow = apiReport.ReportSettings.AttributionWindow,
                Version = apiReport.ReportSettings.Version,
                EntityTypes = apiReport.ReportSettings.EntityTypes,
                EntityStatus = apiReport.ReportSettings.EntityStatus,
            };

            try
            {
                _walmartOnsiteService.GenerateSnapshotAsync(apiReportItem).GetAwaiter().GetResult();

                apiReportItem.SnapShotIDGenerated = DateTime.UtcNow;
                if (apiReportItem.IsFailed)
                    LogMessage(LogLevel.Warn, $"JsonSerializationException for ReportName: {apiReport.APIReportName} -> QueueID: {queueItem.ID}. Report not available");
                else
                    apiReportItem.SnapshotHasBeenGenerated = true;

                var unfinishedReport = apiReportItems.Where(x => x.QueueID == queueItem.ID && x.ReportName == apiReport.APIReportName && x.ShouldGenerateSnapshot).FirstOrDefault();
                if (unfinishedReport != null)
                {
                    var index = apiReportItems.IndexOf(unfinishedReport);
                    apiReportItems[index] = apiReportItem;
                }
                else
                {
                    apiReportItems.Add(apiReportItem);
                }
            }
            catch (Exception ex)
            {
                apiReportItem.IsFailed = true;
                LogException(LogLevel.Error, $"Failed to submit snapshot report for ReportName: {apiReport.APIReportName} -> QueueID: {queueItem.ID}", ex);
                throw;
            }
            finally
            {
                _unfinishedReportProvider.SaveReport(queueItem.FileGUID.ToString(), apiReportItems.Where(x => x.FileGuid == queueItem.FileGUID));
            }

        }
    }

    private void CheckReportStatus(OrderedQueue queueItem, List<ApiReportItem> apiReports)
    {
        foreach (var apiReport in apiReports.Where(x => !x.IsReadyForDownload && x.QueueID == queueItem.ID && x.SnapShotID != null))
        {
            try
            {

                var reportResponse = _walmartOnsiteService.GetSnapshotStatus(apiReport).GetAwaiter().GetResult();
                switch (reportResponse.JobStatus)
                {
                    case SnapshotJobStatus.Done:
                        apiReport.IsReadyForDownload = true;
                        apiReport.DownloadURI = reportResponse.Details;
                        break;
                    case SnapshotJobStatus.Failed:
                    case SnapshotJobStatus.Expired:
                        apiReport.IsFailed = true;
                        break;
                    case SnapshotJobStatus.Pending:
                    case SnapshotJobStatus.Processing:
                        //Do nothing
                        break;
                    default:
                        throw new APIReportException("Snapshot Job Status not recognized");
                }
            }
            catch (Exception ex)
            {
                apiReport.IsFailed = true;
                LogException(LogLevel.Error, $"Failed to submit snapshot report for ReportName: {apiReport.ReportName} -> QueueID: {queueItem.ID}", ex);
                throw;
            }
            finally
            {
                _unfinishedReportProvider.SaveReport(queueItem.FileGUID.ToString(), apiReports.Where(x => x.FileGuid == queueItem.FileGUID));
            }
        }
    }

    private void DownloadReports(OrderedQueue queueItem, List<ApiReportItem> apiReports)
    {
        var reportsReadyForDownload = apiReports.Where(x => x.QueueID == queueItem.ID && x.IsReadyForDownload && !x.IsDownloaded).ToList();

        if (reportsReadyForDownload.Count == 0)
        {
            return;
        }

        ConcurrentQueue<Exception> exceptions = new();

        ThrottleCalls(reportsReadyForDownload, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
        {
            Parallel.ForEach(reports, _apiParallelOptions, apiReportItem =>
            {
                try
                {
                    using Stream responseStream = _walmartOnsiteService.DownloadReportsAsync(apiReportItem).GetAwaiter().GetResult();
                    string fileName = $"{apiReportItem.FileGuid}_{apiReportItem.ReportName}.{apiReportItem.FileExtension}";
                    string[] paths =
                    [
                        queueItem.EntityID.ToLower(),
                        GetDatedPartition(apiReportItem.ReportDate),
                        fileName
                    ];

                    UploadToRawFolder(responseStream, paths);
                    UploadToStageFolderAsync(responseStream, paths).GetAwaiter().GetResult();
                    apiReportItem.IsDownloaded = true;
                }
                catch (HttpClientProviderRequestException ex)
                {
                    LogException(LogLevel.Error, $"|Exception details : {ex}", ex);
                    exceptions.Enqueue(ex);
                }
                catch (Exception ex)
                {
                    LogException(LogLevel.Error, $"{ex.Message}", ex);
                    exceptions.Enqueue(ex);
                }
            });
        });

        _unfinishedReportProvider.SaveReport(queueItem.FileGUID.ToString(), apiReports.Where(x => x.FileGuid == queueItem.FileGUID));

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }
    }

    private void UploadToRawFolder(Stream responseStream, string[] paths)
    {
        responseStream.Seek(0, SeekOrigin.Begin);
        StreamFile incomingFile = new(responseStream, GreenhouseS3Creds, autoCloseStream: false);
        S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);
    }

    private async Task UploadToStageFolderAsync(Stream responseStream, string[] paths)
    {
        responseStream.Seek(0, SeekOrigin.Begin);

        using StreamReader reader = new(responseStream);
        string content = await reader.ReadToEndAsync();
        bool noReportData = ReportDataIsUnavailable(content);

        responseStream.Seek(0, SeekOrigin.Begin);

        //If there is no report data, use an empty stream so that we generate an empty file in stage
        await using Stream stageStream = noReportData ? new MemoryStream() : responseStream;
        StreamFile incomingFile = new(UnzipFile(stageStream, paths), GreenhouseS3Creds);

        S3File rawFile = new(RemoteUri.CombineUri(_baseStageDestUri, paths), GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);
    }

    private static Stream UnzipFile(Stream stream, string[] paths)
    {
        if (paths[2].Split('.').Last() == "zip")
        {
            using var unzip = new ICSharpCode.SharpZipLib.Zip.ZipFile(stream);
            foreach (ZipEntry zip in unzip)
            {

                var extension = zip.Name.Split('.').Last();
                paths[2] = paths[2].Split('.').First() + '.' + extension;
                var zipStream = unzip.GetInputStream(zip);

                var memoryStream = new MemoryStream();
                byte[] buffer = new byte[4096];
                int bytesRead;

                while ((bytesRead = zipStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    memoryStream.Write(buffer, 0, bytesRead);
                }

                memoryStream.Position = 0;

                return memoryStream;
            }
        }
        return stream;
    }

    private static void ThrottleCalls<T>(List<T> source, int nbItemsPerMinute, Action<string> logInfo, Action<IEnumerable<T>> action)
    {
        const int oneMinuteInMilliseconds = 60 * 1000;
        var importStopWatch = System.Diagnostics.Stopwatch.StartNew();
        var subLists = UtilsText.GetSublistFromList(source, nbItemsPerMinute);
        foreach (var list in subLists)
        {
            action(list);

            // have we made _maxAPIRequestPer60s calls in less than a minute? if so we wait
            long remainingTime = oneMinuteInMilliseconds - importStopWatch.ElapsedMilliseconds;
            if (remainingTime > 0)
            {
                logInfo($"Queries per minute quota reached - Pausing for {remainingTime} ms");
                Task.Delay((int)remainingTime).Wait();
            }
            importStopWatch = System.Diagnostics.Stopwatch.StartNew();
        }
    }

    private static bool ReportDataIsUnavailable(string response)
    {
        return response.Contains("ERROR \nNo report data available for this", StringComparison.OrdinalIgnoreCase);
    }

    private bool HasRuntimeExceeded()
    {
        bool hasRunTimeExceeded = _runTime.Elapsed > _maxRuntime;
        if (!hasRunTimeExceeded)
        {
            return false;
        }

        _warningCount++;
        LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
        return true;
    }

    private void LogMessage(LogLevel logLevel, string message)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
    }

    private void LogException(LogLevel logLevel, string message, Exception exc = null)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            //TODO: dispose required objects
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }

    public void PostExecute() { }
}

using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.DAL.DataSource.TikTok;
using Greenhouse.Data.DataSource;
using Greenhouse.Data.DataSource.TikTok;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.TikTok;

[Export("TikTok-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private Uri _baseDestUri;
    private List<IFileItem> _queueItems;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private string _apiVersion;
    private int _resetSeconds;
    private readonly Stopwatch _runtime = new Stopwatch();
    private TimeSpan _maxRuntime;
    private int _exceptionCount;
    private int _backOffMaxRetry;
    private int _maxRetry;
    private Action<LogLevel, string> _log;
    private Action<LogLevel, string, Exception> _logEx;
    // by using a static property, all instances ( = all integrations) will share a reference to the same object
    // using a lock on that object means that only 1 instance at a time can execute the code within the lock
    private static readonly object _classLock = new object();
    private readonly QueueServiceThreadLock _queueServiceThreadLock = new QueueServiceThreadLock(_classLock);
    private List<APIReportItem> _unfinishedReports;
    private UnfinishedReportProvider<APIReportItem> _unfinishedReportProvider;

    private TikTokService _tikTokService;
    private int _requestPageSize;
    private ReportState _dimensionReportState;
    private DateTime _jobRunDateTime;
    private readonly List<string> _badEntity = new();
    private int _totalDaysValid;
    private int _asyncSleepSeconds;

    private string _asyncSubmitReportUrl;
    private string _asyncStatusUrl;
    private string _asyncDownloadUrl;

    private const string FACT_CSV_ASYNC_REPORT_FILE_EXTENSION = "csv";


    private string DimensionReportStateKey => $"{Constants.TIKTOK_DIMENSION_REPORT_STATE}_{CurrentIntegration.IntegrationID}";

    public void PreExecute()
    {
        base.Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();

        Logger logger = NLog.LogManager.GetCurrentClassLogger();
        _log = (logLevel, msg) => logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(msg)));
        _logEx = (logLevel, msg, ex) => logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(msg), ex));

        _log(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        int parentIntegrationID = CurrentIntegration.ParentIntegrationID ?? CurrentIntegration.IntegrationID;
        _queueItems = _queueServiceThreadLock.GetOrderedTopQueueItemsByCredential(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.CredentialID, parentIntegrationID)?.ToList();
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        _apiVersion = CurrentCredential.ConnectionStringDecrypted.Contains("version", StringComparison.CurrentCultureIgnoreCase) ? CurrentCredential.CredentialSet?.Version : "v1.3";

        _asyncStatusUrl = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_ASYNC_CALLS_CHECK_STATUS_URL, "reports/task/check").TrimEnd('/');
        _asyncDownloadUrl = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_ASYNC_CALLS_DOWNLOAD_URL, "reports/task/download").TrimEnd('/');
        _asyncSubmitReportUrl = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_ASYNC_CALLS_SUBMIT_REPORT_URL, "report/task/create");

        _requestPageSize = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_PAGE_SIZE, 1000);
        _resetSeconds = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_THREAD_SLEEP, 60);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _backOffMaxRetry = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_BACKOFF_MAX_RETRY, 2);
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_POLLY_MAX_RETRY, 10);
        _totalDaysValid = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_ASYNC_TASK_TOTAL_DAYS_VALID, 30);
        _asyncSleepSeconds = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_ASYNC_SLEEP, 1);
        int maxParallelAPI = LookupService.GetLookupValueWithDefault(Constants.TIKTOK_MAX_PARALLEL_IMPORT, 2);

        var apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxParallelAPI };

        _dimensionReportState = LookupService.GetAndDeserializeLookupValueWithDefault(DimensionReportStateKey, new ReportState());

        _jobRunDateTime = DateTime.Now.Date;

        if (_dimensionReportState.DateReportSubmitted < _jobRunDateTime)
        {
            _dimensionReportState.DateReportSubmitted = _jobRunDateTime;
            _dimensionReportState.APIEntitiesSubmitted = new HashSet<string>();
        }

        _unfinishedReportProvider = new UnfinishedReportProvider<APIReportItem>(_baseDestUri, _log, _logEx);

        CleanupUnfinishedReports();

        _unfinishedReports = _unfinishedReportProvider.LoadUnfinishedReportsFile(_queueItems);

        var options = new TikTokServiceOptions
        {
            HostURI = CurrentIntegration.EndpointURI,
            Version = _apiVersion,
            Token = CurrentCredential.CredentialSet.AccessToken,
            ThreadSleep = _resetSeconds,
            ParallelOptions = apiParallelOptions,
            PageSize = _requestPageSize
        };

        _tikTokService = new TikTokService(options, HttpClientProvider);
    }

    public void Execute()
    {
        _runtime.Start();

        if (_queueItems.Count == 0)
        {
            _log(LogLevel.Info, "There are no reports in the Queue");
            return;
        }

        foreach (Queue queue in _queueItems)
        {
            if (HasRuntimeExceededLimit())
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = "Job RunTime exceeded max runtime.";
                break;
            }

            if (_badEntity.Contains(queue.EntityID))
            {
                _log(LogLevel.Info, $"Queue ID={queue.ID} with FileGUID={queue.FileGUID} is skipped as APIEntity={queue.EntityID} failed for a previous queue");
                continue;
            }

            List<APIReportItem> reportList = new();

            try
            {
                GenerateReports(reportList, _apiReports, queue);
                DownloadReports(reportList, queue);

                UpdateUnfinishedQueues(reportList, queue);
            }
            catch (HttpClientProviderRequestException ex)
            {
                HandleException(queue, reportList, ex);
            }
            catch (Exception ex)
            {
                HandleException(queue, reportList, ex);
            }
        }

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }

        _log(LogLevel.Info, "Import job complete");
    }

    private void HandleException<TException>(Queue queue, List<APIReportItem> reportList, TException exception) where TException : Exception
    {
        _exceptionCount++;
        _badEntity.Add(queue.EntityID);
        SaveUnfinishedReports(reportList, queue);
        JobService.UpdateQueueStatus(queue.ID, Constants.JobStatus.Error);

        string logMessage = BuildLogMessage(exception);
        _logEx(LogLevel.Error, logMessage, exception);
    }

    private static string BuildLogMessage<TException>(TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"TikTok message error | Exception details : {httpEx}",
            _ => $"TikTok message error {exception.Message}"
        };
    }

    private void CleanupUnfinishedReports()
    {
        var activeGuids = JobService.GetQueueGuidBySource(CurrentSource.SourceID);

        //Remove any unfinished report files whose queues were deleted
        _unfinishedReportProvider.CleanupReports(_baseDestUri, activeGuids);
    }

    private void UpdateUnfinishedQueues(List<APIReportItem> reportList, Queue queueItem)
    {
        //Do not mark queue as Pending if any of the downloads failed
        if (reportList.Any(x => x.HasFailedToDownload))
        {
            return;
        }

        var unfinishedReports = reportList.Where(x => !x.IsDownloaded);

        if (unfinishedReports.Any())
        {
            _log(LogLevel.Warn, $"There are unfinished reports. There are {_exceptionCount} error counts.  Updating queue status to 'Pending': {JsonConvert.SerializeObject(unfinishedReports)}");

            base.UpdateQueueWithDelete(new List<IFileItem> { queueItem }, Common.Constants.JobStatus.Pending, false);
        }
        else
        {
            _log(LogLevel.Info, "No unfinished report.");
        }
    }

    private void SaveUnfinishedReports(List<APIReportItem> reportList, Queue queue)
    {
        long queueID = queue.ID;
        string fileGuid = queue.FileGUID.ToString();

        var reportsForQueue = reportList.Where(x => x.QueueID == queueID);

        _unfinishedReportProvider.SaveReport(fileGuid, reportsForQueue);

        _log(LogLevel.Info, $"Stored unfinished reports for queueID: {queueID} and fileGUID: {fileGuid} in S3");
    }

    private void DownloadReports(List<APIReportItem> reportList, Queue queueItem)
    {
        if (reportList.Count == 0)
        {
            _log(LogLevel.Info, "There are no reports to run");
            return;
        }

        var reports = reportList.Where(x => !x.IsDownloaded).OrderBy(x => x.IsDimension).ToList();

        foreach (var reportItem in reports)
        {
            if (HasRuntimeExceededLimit())
            {
                return;
            }

            try
            {
                if (reportItem.IsDimension)
                {
                    DownloadDimensionReport(reportItem, queueItem);
                }
                else if (queueItem.IsBackfill && !reportItem.ReportSettings.UseSyncApiOnly)
                {
                    DownloadCSVFactReport(reportItem, queueItem);
                }
                else
                {
                    DownloadFactReportAsync(reportItem, queueItem).GetAwaiter().GetResult();
                }

                var queueReportList = reportList.Where(x => x.QueueID == queueItem.ID);
                bool done = reportList.All(x => x.IsDownloaded == true);

                if (done)
                {
                    MarkQueueAsComplete(queueItem, queueReportList, reportItem);
                }
                else
                {
                    SaveUnfinishedReports(reportList, queueItem);
                }
            }
            catch (HttpClientProviderRequestException requestException)
            {
                FailDownloadAndLogError(reportItem, requestException);
                throw;
            }
            catch (Exception exc)
            {
                FailDownloadAndLogError(reportItem, exc);
                throw;
            }//end try catch
        } //end for


        //Check to see if all dimension reports have been downloaded
        var dimensionReportItemList = reportList.Where(x => x.IsDimension);
        if (dimensionReportItemList.Any() && dimensionReportItemList.All(x => x.IsDownloaded))
        {
            _dimensionReportState.APIEntitiesSubmitted.Add(queueItem.EntityID);
            SaveReportState(DimensionReportStateKey, _dimensionReportState);
        }
    }

    private void FailDownloadAndLogError<TException>(APIReportItem reportItem, TException requestException) where TException : Exception
    {
        reportItem.HasFailedToDownload = true;

        string logMessage = BuildLogMessage(reportItem, requestException);

        _logEx(LogLevel.Error, base.PrefixJobGuid(logMessage), requestException);
    }
    private static string BuildLogMessage<TException>(APIReportItem reportItem, TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"Error downloading report - failed on queueID: {reportItem.QueueID} " +
           $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.ReportName} - |Exception details : {httpEx}",
            _ => $"Error downloading report - failed on queueID: {reportItem.QueueID} " +
                    $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.ReportName}  - Exception: {exception.Message} - STACK {exception.StackTrace}"
        };
    }
    private void SaveReportState(string reportStateKey, ReportState reportState)
    {
        var lookup = new Lookup
        {
            Name = reportStateKey,
            Value = JsonConvert.SerializeObject(reportState),

            LastUpdated = _jobRunDateTime,
            IsEditable = false
        };

        Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
        Data.Repositories.LookupRepository.AddOrUpdateLookup(lookup);
    }

    private void MarkQueueAsComplete(Queue queueItem, IEnumerable<APIReportItem> queueReportList, APIReportItem reportItem)
    {
        _log(LogLevel.Debug,
            $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; " +
            $"file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}");

        var files = queueReportList.Where(x => x.FileItem != null).Select(x => x.FileItem).ToList();
        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
        queueItem.FileSize = files.Sum(x => x.FileSize);
        queueItem.DeliveryFileDate = queueReportList.Max(x => x.FileDate);

        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update<Queue>((Queue)queueItem);
        _unfinishedReportProvider.DeleteReport(reportItem.FileGuid.ToString());
    }

    private async Task DownloadFactReportAsync(APIReportItem reportItem, Queue queueItem)
    {
        ConcurrentBag<FileCollectionItem> factDownloadConcurrentBag = new();
        ConcurrentBag<DateTime> factRawFileDownloadTimes = new();

        var downloadRequest = GenerateApiReportRequest(queueItem, reportItem, HttpMethod.Get, reportItem.ReportSettings.Path.TrimEnd('/'));

        await _tikTokService.DownloadFactReportAsync(downloadRequest,
            threadSleepAction: LogThreadSleep,
            saveFileAction: (stream, groupNumber, pageNumber) =>
            {
                var fileName =
                    $"{queueItem.FileGUID}_{reportItem.ReportName}_{groupNumber}_{pageNumber}.{reportItem.FileExtension}";

                var fileItemAndWriteTime = UploadFileToS3(queueItem, fileName, reportItem.ReportName, stream);

                factDownloadConcurrentBag.Add(fileItemAndWriteTime.fileItem);
                factRawFileDownloadTimes.Add(fileItemAndWriteTime.fileLastWriteTime);

            });

        reportItem.IsDownloaded = true;

        var factDownloadList = factDownloadConcurrentBag.ToList();

        reportItem.FileCollection = factDownloadList;
        reportItem.FileDate = factRawFileDownloadTimes.ToList().Max(x => x);

        var manifestFile = CreateManifestFile((Queue)queueItem, factDownloadList, reportItem.ReportName);
        reportItem.FileItem = manifestFile;
    }

    private void DownloadDimensionReport(APIReportItem reportItem, Queue queueItem)
    {

        List<FileCollectionItem> dimensionDownloadList = new();
        GetDimensionReport(queueItem, reportItem, dimensionDownloadList);

        reportItem.FileCollection = dimensionDownloadList;

        if (reportItem.ReportSettings.IsAccountInfo)
        {
            var fileCollectionItem = new FileCollectionItem()
            {
                FileSize = dimensionDownloadList.Sum(file => file.FileSize),
                SourceFileName = reportItem.ReportName.ToLower(),
                FilePath = $"{queueItem.FileGUID}_{reportItem.ReportName}.{reportItem.FileExtension}"
            };

            reportItem.FileItem = fileCollectionItem;
        }
        else
        {
            var manifestFile = CreateManifestFile((Queue)queueItem, dimensionDownloadList, reportItem.ReportName);
            reportItem.FileItem = manifestFile;
        }

    }

    private void GetDimensionReport(Queue queueItem, APIReportItem reportItem, List<FileCollectionItem> dimensionDownloadList)
    {
        var reportRequest = GenerateApiReportRequest(queueItem, reportItem, HttpMethod.Get, reportItem.ReportSettings.Path.TrimEnd('/'));

        _tikTokService.DownloadReportAsync(reportRequest,
            threadSleepAction: (message) =>
            {
                LogThreadSleep(message);
            },
            saveFileAction: (stream, pageNumber) =>
            {
                var fileName = reportItem.ReportSettings.IsAccountInfo ?
                    $"{queueItem.FileGUID}_{reportItem.ReportName}.{reportItem.FileExtension}" :
                    $"{queueItem.FileGUID}_{reportItem.ReportName}_{pageNumber}.{reportItem.FileExtension}";


                var fileItemAndWriteTime = UploadFileToS3(queueItem, fileName, reportItem.ReportName, stream);

                dimensionDownloadList.Add(fileItemAndWriteTime.fileItem);

                reportItem.FileDate = fileItemAndWriteTime.fileLastWriteTime;

            }).GetAwaiter().GetResult();

        reportItem.IsDownloaded = true;

    }

    private ApiReportRequest GenerateApiReportRequest(Queue queueItem, APIReportItem reportItem, HttpMethod method, string path)
    {
        var reportRequest = new ApiReportRequest()
        {
            ProfileID = queueItem.EntityID,
            MethodType = method.ToString(),
            ReportPath = CurrentIntegration.EndpointURI + $"/{_apiVersion}/" + path,
            IsAccountInfo = reportItem.ReportSettings.IsAccountInfo,
            StartDate = queueItem.FileDate,
            EndDate = queueItem.FileDate,
            ReportSettings = reportItem.ReportSettings,
            PageSize = reportItem.ReportSettings.PageSize
        };

        var apiReport = _apiReports.Where(x => x.APIReportName.Equals(reportItem.ReportName, StringComparison.CurrentCultureIgnoreCase)).FirstOrDefault();

        if (reportItem.ReportSettings.UseMetrics)
        {
            reportRequest.Metrics = (reportItem.ReportSettings.UseMetrics && apiReport.ReportFields.Any()) ? apiReport.ReportFields.Where(x => !x.IsDimensionField) : null;
        }

        if (reportItem.ReportSettings.UseDimensions)
        {
            reportRequest.Dimensions = (reportItem.ReportSettings.UseDimensions && apiReport.ReportFields.Any()) ? apiReport.ReportFields.Where(x => x.IsDimensionField) : null;
        }

        reportRequest.SetParameters();

        return reportRequest;
    }

    private void GenerateReports(List<APIReportItem> reportList, IEnumerable<APIReport<ReportSettings>> reports, Queue queueItem)
    {
        if (HasRuntimeExceededLimit())
        {
            return;
        }

        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

        //Check if the reports have already been submitted (unfinished)
        var reportsForQueueNotFullyDownloaded = _unfinishedReports.Where(r => r.QueueID == queueItem.ID && !r.IsDownloaded).ToList();

        //If there are any unfinished reports not marked as dowloaded, then there might be files that were downloaded as part of that report.
        //We need to remove those files to prevent duplicates
        foreach (var reportItem in reportsForQueueNotFullyDownloaded)
        {
            string[] paths =
            [
                    queueItem.EntityID.ToLower(),
                GetDatedPartition(queueItem.FileDate)
            ];
            DeletePartiallyDownloadedReports(paths, reportItem.FileGuid + "_" + reportItem.ReportName.ToLower() + "_");
        }

        var unfinishedForQueue = _unfinishedReports.Where(r => r.QueueID == queueItem.ID).ToList();
        // per doc: An asynchronous report task is valid for 30 days, after which you cannot query for its status or download its output.
        // we remove these from unfinished reports
        var minus30Date = DateTime.UtcNow.AddDays(-_totalDaysValid);
        unfinishedForQueue.RemoveAll(u => !u.IsDimension && u?.TaskRunDate < minus30Date && !u.ReportSettings.UseSyncApiOnly && !u.IsDownloaded);

        foreach (var report in reports)
        {
            var alreadyCreatedReport = unfinishedForQueue.FirstOrDefault(r => r.APIReportID == report.APIReportID && (!r.HasFailedToDownload || r.IsDownloaded));

            if (alreadyCreatedReport != null)
            {
                reportList.Add(alreadyCreatedReport);
                continue;
            }

            if (report.ReportSettings.IsDimensionReport)
            {
                //Check to see if dimension reports were already downloaded today for this entity. If so, skip it
                if (_dimensionReportState.APIEntitiesSubmitted.Contains(queueItem.EntityID))
                {
                    _log(LogLevel.Info, $"Dimension data for: {queueItem.EntityID} already downloaded today. Skipping.");
                    continue;
                }

                var dimensionReportItem = GenerateDimensionReport(queueItem, report);
                reportList.Add(dimensionReportItem);
                continue;
            }

            var reportItem = GenerateFactReport(queueItem, report);
            reportList.Add(reportItem);
        }

        SaveUnfinishedReports(reportList, queueItem);

    }

    #region Async Report Downloads


    private void DownloadReport(APIReportItem reportItem, ApiReportRequest reportRequest, FileCollectionItem fileItem
        , out DateTime? deliveryFileDate)
    {
        DateTime? fileDate = DateTime.Now;
        deliveryFileDate = fileDate;

        try
        {
            var queueItem = _queueItems.Find(q => q.ID == reportItem.QueueID);

            _tikTokService.DownloadReportAsync(reportRequest,
                threadSleepAction: (message) =>
                {
                    LogThreadSleep(message);
                },
                saveFileAction: (stream, pageNumber) =>
                {
                    reportItem.FileName = $"{queueItem.FileGUID}_{reportItem.ReportName}.{reportItem.FileExtension}";
                    _log(LogLevel.Info, $"{CurrentSource.SourceName} start DownloadReport: queueID: {queueItem.ID}->{reportItem.ReportToken}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {reportItem.FileName}");

                    string[] paths =
                    [
                        queueItem.EntityID.ToLower(),
                        GetDatedPartition(queueItem.FileDate),
                        reportItem.FileName
                    ];

                    S3File rawFile = new S3File(RemoteUri.CombineUri(this._baseDestUri, paths), GreenhouseS3Creds);
                    var incomingFile = new StreamFile(stream, GreenhouseS3Creds);
                    base.UploadToS3(incomingFile, rawFile, paths);

                    fileItem.FileSize = rawFile.Length;
                    fileItem.SourceFileName = reportItem.ReportName;
                    fileItem.FilePath = reportItem.FileName;

                    fileDate = UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, rawFile.LastWriteTimeUtc);

                }).GetAwaiter().GetResult();

            deliveryFileDate = fileDate;

            _log(LogLevel.Info, $"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{reportItem.ReportToken}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {reportItem.FileName}");

        }
        catch (WebException wex)
        {
            _exceptionCount++;
            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
            _logEx(LogLevel.Error, $"Web Exception Error downloading report - failed on queueID: {reportItem.QueueID} " +
                    $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.ReportName} ->" +
                    $"Error -> Exception: {wex.Message}", wex);

            throw;
        }
        catch (Exception exc)
        {
            _exceptionCount++;
            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
            _logEx(LogLevel.Error,
                    $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} " +
                    $" ReportToken: {reportItem.ReportToken} Report Name: {reportItem.ReportName}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}", exc);

            throw;
        }
    }

    private void DownloadCSVFactReport(APIReportItem reportItem, Queue queueItem)
    {
        CheckStatus(queueItem, reportItem);

        if (reportItem.Status == ReportStatus.SUCCESS.ToString())
        {
            var downloadRequest = new ApiReportRequest()
            {
                IsTask = true,
                ProfileID = reportItem.ProfileID,
                ReportToken = reportItem.ReportToken,
                MethodType = System.Net.Http.HttpMethod.Get.ToString(),
                ReportPath = CurrentIntegration.EndpointURI + $"/{_apiVersion}/" + _asyncDownloadUrl
            };

            var fileItem = new FileCollectionItem();
            var deliveryFileDate = new DateTime?();
            DownloadReport(reportItem, downloadRequest, fileItem, out deliveryFileDate);

            reportItem.IsDownloaded = true;
            reportItem.FileItem = fileItem;
            reportItem.FileDate = deliveryFileDate;
        }
        else if (reportItem.Status == ReportStatus.FAILED.ToString()) /* Error out Queue */
        {
            //Error queue Item, if any of its report types failed to download
            queueItem.StatusId = (int)Constants.JobStatus.Error;
            reportItem.HasFailedToDownload = true;
            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
            throw new APIResponseException("CSV report failed to download");
        }
        else if (reportItem.Status == null)
        {
            reportItem.HasFailedToDownload = true;
            reportItem.Status = ReportStatus.FAILED.ToString();
        }
    }

    private void CheckStatus(Queue queueItem, APIReportItem reportItem)
    {
        var reportRequest = new ApiReportRequest()
        {
            IsTask = true,
            ProfileID = reportItem.ProfileID,
            ReportToken = reportItem.ReportToken,
            ReportPath = CurrentIntegration.EndpointURI + $"/{_apiVersion}/" + _asyncStatusUrl
        };

        var status = _tikTokService.CheckReportStatus(reportRequest).GetAwaiter().GetResult();

        reportItem.Status = status ?? null;
        _log(LogLevel.Info, $"Check Report Status: FileGUID: {reportItem.FileGuid}->API Status: {status}");
    }
    #endregion

    private void DeletePartiallyDownloadedReports(string[] paths, string match)
    {
        var rac = GetS3RemoteAccessClient();
        var uri = GetUri(paths, Constants.ProcessingStage.RAW);
        var dir = rac.WithDirectory(uri);
        var files = dir.GetFiles();

        Regex regex = new Regex($@"{match}[\d]");

        var matchingFiles = files.Where(x => regex.IsMatch(x.Name));
        var nbFiles = matchingFiles.Count();

        _log(LogLevel.Info, $"Start deleting {nbFiles} Files matching '{match}' in folder {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName};");
        matchingFiles.ToList().ForEach(m => m.Delete());
        _log(LogLevel.Info, $"End deleting {nbFiles} Files matching '{match}' in folder  {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName};");
    }

    private APIReportItem GenerateFactReport(Queue queueItem, APIReport<ReportSettings> report)
    {
        var apiReportItem = new APIReportItem()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ProfileID = queueItem.EntityID,
            FileExtension = report.ReportSettings.FileExtension,
            TaskRunDate = DateTime.UtcNow,
            ReportName = report.APIReportName.ToLower(),
            APIReportID = report.APIReportID,
            ReportSettings = report.ReportSettings
        };

        if (queueItem.IsBackfill && !report.ReportSettings.UseSyncApiOnly)
        {
            var reportRequest = GenerateApiReportRequest(queueItem, apiReportItem, HttpMethod.Post, _asyncSubmitReportUrl);
            var taskId = _tikTokService.SubmitReportAsync(
                reportRequest,
                (httpRequestMessage) =>
                {
                    _log(LogLevel.Info, $"Submitting Async Report Generation: RequestUri: {httpRequestMessage.RequestUri}");
                })
                .GetAwaiter().GetResult();

            apiReportItem.ReportToken = taskId;
            apiReportItem.FileExtension = FACT_CSV_ASYNC_REPORT_FILE_EXTENSION;

            // Due to computing resource limits, the rate limit for each app is 0.3 QPS. 
            Task.Delay(1000 * _asyncSleepSeconds).Wait();
        }

        return apiReportItem;
    }

    private static APIReportItem GenerateDimensionReport(Queue queueItem, APIReport<ReportSettings> report)
    {
        return new APIReportItem()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ProfileID = queueItem.EntityID,
            FileExtension = report.ReportSettings.FileExtension,
            IsDimension = true,
            ReportName = report.APIReportName.ToLower(),
            APIReportID = report.APIReportID,
            ReportSettings = report.ReportSettings
        };
    }

    private FileCollectionItem CreateManifestFile(Queue queueWithData, List<FileCollectionItem> fileItems, string fileType, Queue currentQueue = null)
    {
        var manifest = new Data.Model.Setup.RedshiftManifest();

        currentQueue = currentQueue ?? queueWithData;

        foreach (var file in fileItems)
        {
            var s3File = $"{_baseDestUri.OriginalString.TrimStart('/')}/{queueWithData.EntityID.ToLower()}/{GetDatedPartition(queueWithData.FileDate)}/{file.FilePath}";
            manifest.AddEntry(s3File, true);
        }

        var fileName = $"{currentQueue.FileGUID}_{fileType}.manifest";
        var manifestPath = GetManifestFilePath(currentQueue, fileName);
        var manifestFilePath = ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);

        var fileItem = new FileCollectionItem()
        {
            FileSize = fileItems.Sum(file => file.FileSize),
            SourceFileName = fileType,
            FilePath = fileName
        };

        return fileItem;
    }

    private string[] GetManifestFilePath(Queue queueItem, string name)
    {
        string[] manifestPath = new string[]
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate)
        };

        var manifestUri = RemoteUri.CombineUri(_baseDestUri, manifestPath);
        return new string[]
        {
            manifestUri.AbsolutePath, name
        };
    }

    private void LogThreadSleep(string message)
    {
        _log(LogLevel.Info, message);
    }

    private bool HasRuntimeExceededLimit()
    {
        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
        {
            _log(LogLevel.Warn, $"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
            return true;
        }

        return false;
    }

    private (FileCollectionItem fileItem, DateTime fileLastWriteTime) UploadFileToS3(Queue queueItem,
        string fileName, string reportName, Stream stream)
    {
        string[] paths =
        [
            queueItem.EntityID.ToLower(),
            GetDatedPartition(queueItem.FileDate),
            fileName
        ];

        stream.Seek(0, SeekOrigin.Begin);

        S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
        StreamFile incomingFile = new(stream, GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);

        return (new FileCollectionItem { FileSize = rawFile.Length, SourceFileName = reportName, FilePath = fileName },
            rawFile.LastWriteTimeUtc);
    }

    public void PostExecute()
    {
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
            // Add here what needs to be disposed
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}

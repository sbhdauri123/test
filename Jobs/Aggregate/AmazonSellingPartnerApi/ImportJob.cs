using Greenhouse.Auth;
using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.AmazonSellingPartnerApi;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Request;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
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
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.AmazonSellingPartnerApi;

[Export("AmazonSellingPartnerApi-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private AmazonSellingPartnerApiService _amazonSellingPartnerApiService;
    private List<OrderedQueue> _queueItems;
    private TimeSpan _maxRuntime;
    private int _maxRetry;
    private int _counter;
    private int _seed;
    private int _exceptionCount;
    private int _chunkSize;
    private UnfinishedReportProvider<APIReportItem> _unfinishedReportProvider;
    private Uri _baseDestUri;
    private IHttpClientProvider _httpClientProvider;
    private ITokenCache _tokenCache;
    private ITokenApiClient _tokenApiClient;
    private readonly Stopwatch _runTime = new();
    private ParallelOptions _apiParallelOptions;
    private int _maxAPIRequestPerMinute;
    private int _maxDegreeOfParallelism;

    private bool HasRuntimeExceeded() => TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1;

    public void PreExecute()
    {
        _httpClientProvider = base.HttpClientProvider;
        _tokenCache ??= base.TokenCache;
        _tokenApiClient = new TokenApiClient(_httpClientProvider, _tokenCache, CurrentCredential);

        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();

        LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_MAX_RUNTIME
                                               , new TimeSpan(0, 1, 0, 0));
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_POLLY_MAX_RETRY, 10);
        _counter = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_POLLY_COUNTER, 3);
        _seed = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_BACKOFF_DELAY_SECONDS, 4);
        _chunkSize = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_UPLOAD_REPORT_CHUNK_SIZE, 20);
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_MAX_DEGREE_PARALLELISM, 1);
        _maxAPIRequestPerMinute = LookupService.GetLookupValueWithDefault(Constants.AMAZON_SELLING_PARTNER_API_REQUESTS_PER_MINUTE, 2);
        _apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism };

        var nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID
                                                        , CurrentIntegration.IntegrationID).OrderBy(q => q.RowNumber).ToList();

        _unfinishedReportProvider = new UnfinishedReportProvider<APIReportItem>(_baseDestUri, LogMessage, LogException);

        CleanupReports();
        var amazonSellingPartnerApiServiceArguments = new AmazonSellingPartnerApiServiceArguments(base.HttpClientProvider
                                                        , CurrentCredential, GreenhouseS3Creds
                                                        , GetS3PathHelper, _apiReports
                                                        , UploadToS3, LogMessage, LogException, _tokenApiClient);
        _amazonSellingPartnerApiService = new AmazonSellingPartnerApiService(amazonSellingPartnerApiServiceArguments);
    }

    public void Execute()
    {
        _runTime.Start();

        if (_queueItems.Count == 0)
        {
            LogMessage(LogLevel.Info, "There are no items in the Queue.");
            _runTime.Stop();
            return;
        }

        foreach (var queueItem in _queueItems)
        {
            try
            {
                if (HasRuntimeExceeded())
                {
                    LogRuntimeExceededWarning();
                    break;
                }

                //This is weekly report so we will run only on Sunday.
                if (!(queueItem.FileDate.DayOfWeek == DayOfWeek.Sunday))
                {
                    _apiReports = _apiReports.Where(item => item.APIReportName != "BrandAnalyticsRepeatPurchaseReport").ToList();
                }

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                LogMessage(LogLevel.Info, $"{nameof(AmazonSellingPartnerApiService)} initialized.");

                // Check if the reports have already been submitted (unfinished)
                List<APIReportItem> unfinishedForQueue = _unfinishedReportProvider.GetReports(queueItem.FileGUID.ToString())?.ToList() ?? new List<APIReportItem>();

                if (unfinishedForQueue?.Count == 0)
                {
                    // no unfinish data for the queue, this is the begining of the process
                    // let's clean any old data in the S3 raw folder
                    DeleteRawFiles(queueItem);
                }

                List<APIReportItem> reportsList = new(); // Newly created reports
                ConcurrentBag<APIReportItem> concurrentReportsList = new();
                ConcurrentQueue<Exception> exceptions = new();

                foreach (var report in unfinishedForQueue.Where(x => !string.IsNullOrEmpty(x.ReportId)))
                {
                    concurrentReportsList.Add(report);
                }

                // Process only new reports that are not already created
                var remainingReports = _apiReports.Where(apiReport => !unfinishedForQueue.Any(x => x.APIReportID == apiReport.APIReportID
                                                                                            && !string.IsNullOrEmpty(x.ReportId))).ToList();

                ThrottleCalls(remainingReports, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
                {
                    Parallel.ForEach(reports, _apiParallelOptions, (apiReport, state) =>
                    {
                        try
                        {
                            var backOffStrategy = new MultiplicativeBackOffStrategy { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
                            var actionCancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);
                            var newReport = CreateReportForQueueItem(apiReport, queueItem, actionCancellableRetry.Execute).GetAwaiter().GetResult();

                            if (newReport != null)
                            {
                                concurrentReportsList.Add(newReport);
                            }
                        }
                        catch (HttpClientProviderRequestException ex)
                        {
                            LogException(LogLevel.Error, $"|Exception details : {ex}", ex);
                            exceptions.Enqueue(ex);
                            state.Stop();
                        }
                        catch (Exception ex)
                        {
                            LogException(LogLevel.Error, $"{ex.Message}", ex);
                            exceptions.Enqueue(ex);
                            state.Stop();
                        }
                    });
                });

                if (!exceptions.IsEmpty)
                {
                    ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
                }
                // Convert back to List after parallel execution
                reportsList = concurrentReportsList.ToList();

                SaveUnfinishedReports(reportsList);

                reportsList = GetReportStatusAndDocumentId(reportsList, queueItem);

                if (reportsList.Count > 0)
                {
                    CheckReportStatusAndDownload(reportsList, queueItem);
                }
                else
                {
                    LogMessage(LogLevel.Info, $"There are no newly created and unfinished reports available.");
                    MarkQueueAsComplete(queueItem, reportsList);
                }
            }
            catch (Exception ex)
            {
                _exceptionCount++;
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                LogException(LogLevel.Error, $"Report processing failed: GUID: {queueItem.FileGUID}. Exception: {ex.Message}", ex);
            }
        }
        LogMessage(LogLevel.Info, $"{nameof(AmazonSellingPartnerApiService)} finalized.");

        _runTime.Stop();
        LogMessage(LogLevel.Info, $"Import job completed. Took {_runTime.Elapsed}ms to finish.");

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
        if (HasRuntimeExceeded())
        {
            LogRuntimeExceededWarning();
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = "Unfinished reports. Some reports were not ready during this Import and will be picked up by the next one.";
        }
    }

    private void LogRuntimeExceededWarning()
    {
        LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
    }

    private List<APIReportItem> GetReportStatusAndDocumentId(List<APIReportItem> apiReportsList, OrderedQueue queueItem)
    {
        List<APIReportItem> reportList = new List<APIReportItem>();
        ConcurrentBag<APIReportItem> concurrentReportList = new();
        ConcurrentQueue<Exception> exceptions = new();

        var alreadyDownloadedReports = apiReportsList
            .Where(apiReport => apiReport.IsDownloaded || string.IsNullOrEmpty(apiReport.ReportId))
            .ToList();

        foreach (var report in alreadyDownloadedReports)
        {
            concurrentReportList.Add(report);
        }

        //Only process the remaining reports
        var reportsToProcess = apiReportsList.Except(alreadyDownloadedReports).ToList();

        ThrottleCalls(reportsToProcess, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
        {
            Parallel.ForEach(reports, _apiParallelOptions, (apiReport, state) =>
            {
                try
                {
                    var backOffStrategy = new MultiplicativeBackOffStrategy { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);

                    var reportProcessingStatus = _amazonSellingPartnerApiService
                        .GetReportStatusAndDocumentIdAsync(apiReport.ReportId, queueItem, apiReport, cancellableRetry.Execute)
                        .GetAwaiter().GetResult();

                    if (reportProcessingStatus != null && string.IsNullOrEmpty(reportProcessingStatus.ReportDocumentId))
                    {
                        var apiReportItem = UpdateReportStatusAsReportCreated(queueItem, apiReport);
                        concurrentReportList.Add(apiReportItem);
                        LogMessage(LogLevel.Info, $"Report is already created. Report Name: {apiReport.ReportName}, " +
                            $"APIReportID: {apiReport.APIReportID}, Source Id: {queueItem.SourceID}, QueueId: {queueItem.ID}");
                        return;
                    }

                    if (reportProcessingStatus != null &&
                        string.Equals(reportProcessingStatus.ProcessingStatus, ReportStatus.DONE.ToString(), StringComparison.OrdinalIgnoreCase))
                    {
                        var apiReportItem = BuildAPIReportItemObject(apiReport, queueItem, reportProcessingStatus, null);
                        concurrentReportList.Add(apiReportItem);
                    }
                    else
                    {
                        var apiReportItem = UpdateReportStatusAsReportCreated(queueItem, apiReport, reportProcessingStatus);
                        concurrentReportList.Add(apiReportItem);
                        LogMessage(LogLevel.Info, $"Report processing status is {reportProcessingStatus.ProcessingStatus}. " +
                            $"Report Name: {apiReport.ReportName}, APIReportID: {apiReport.APIReportID}, " +
                            $"Source Id: {queueItem.SourceID}, QueueId: {queueItem.ID}");
                    }
                }
                catch (HttpClientProviderRequestException ex)
                {
                    LogException(LogLevel.Error, $"|Exception details : {ex}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
                catch (Exception ex)
                {
                    LogException(LogLevel.Error, $"{ex.Message}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
            });
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        // Convert ConcurrentBag back to a List after parallel execution
        reportList = concurrentReportList.ToList();
        return reportList;
    }

    private static APIReportItem UpdateReportStatusAsReportCreated(OrderedQueue queueItem, APIReportItem apiReport, ReportProcessingStatus reportProcessingStatus = null)
    {
        ReportProcessingStatus failedReportsStatus = new ReportProcessingStatus
        {
            ReportId = apiReport.ReportId,
            ProcessingStatus = reportProcessingStatus == null ? InternalReportStatus.ReportCreated.ToString() : reportProcessingStatus.ProcessingStatus,
            MarketplaceIds = apiReport.MarketplaceIds,
            ReportDocumentId = reportProcessingStatus == null ? string.Empty : reportProcessingStatus.ReportDocumentId
        };
        APIReportItem apiReportItem = BuildAPIReportItemObject(apiReport, queueItem, failedReportsStatus, null);
        return apiReportItem;
    }

    public void SaveUnfinishedReports(List<APIReportItem> listAPIReportItem)
    {
        if (listAPIReportItem.Count > 0)
        {
            _unfinishedReportProvider.SaveReport(listAPIReportItem.FirstOrDefault().FileGuid.ToString(), listAPIReportItem);
        }
    }

    private async Task<APIReportItem> CreateReportForQueueItem(APIReport<ReportSettings> apiReport, OrderedQueue queueItem
                                       , Action<Action> cancellableRetry)
    {
        string newReportResponse = string.Empty;
        APIReportItem apiReportItem = new APIReportItem();
        string jsonPayload = ApiReportRequest.PrepareJsonObject(queueItem, apiReport);

        CreateReportResponse createReportResponse = await _amazonSellingPartnerApiService.RequestReportAsync(jsonPayload
                                                                                    , queueItem, apiReport
                                                                                    , cancellableRetry);
        string reportId = createReportResponse?.ReportId ?? string.Empty;
        apiReportItem = BuildAPIReportItemObject(apiReport, queueItem, null, reportId);
        return apiReportItem;
    }

    private static APIReportItem BuildAPIReportItemObject<T>(T apiReport, OrderedQueue queueItem, ReportProcessingStatus newReportResponse = null, string reportIdResponse = null)
    {
        APIReportItem apiReportItem = new APIReportItem
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            MarketplaceId = queueItem.EntityID,
            Status = newReportResponse != null ? newReportResponse.ProcessingStatus : InternalReportStatus.ReportCreated.ToString(),
            IsReady = false,
            IsDownloaded = false,
            FileExtension = ".json",
            FileDate = queueItem.FileDate,
            TaskRunDate = DateTime.Now,
            ReportStartDate = queueItem.FileDate.ToString("yyyy-MM-dd"),
            ReportEndDate = queueItem.FileDate.ToString("yyyy-MM-dd")
        };

        // Handle APIReportItem type
        if (apiReport is APIReportItem apiReportItemObj)
        {
            apiReportItem.ReportId = newReportResponse?.ReportId ?? reportIdResponse;
            apiReportItem.ReportName = apiReportItemObj.ReportName;
            apiReportItem.APIReportID = apiReportItemObj.APIReportID;
            apiReportItem.ReportDocumentId = newReportResponse?.ReportDocumentId ?? string.Empty;

            // MarketplaceIds as List<string>
            apiReportItem.MarketplaceIds = newReportResponse?.MarketplaceIds ?? new List<string>(); // Assign empty list if null
        }
        // Handle APIReport<ReportSettings> type
        else if (apiReport is APIReport<ReportSettings> apiReportObj)
        {
            apiReportItem.ReportId = reportIdResponse;
            apiReportItem.ReportName = apiReportObj.APIReportName;
            apiReportItem.APIReportID = apiReportObj.APIReportID;
            apiReportItem.ReportDocumentId = string.Empty;

            // MarketplaceIds as List<string>
            apiReportItem.MarketplaceIds = new List<string>(); // Assign empty list as default
        }
        return apiReportItem;
    }

    private void CheckReportStatusAndDownload(List<APIReportItem> reportsList, OrderedQueue queueItem)
    {
        List<APIReportItem> updatedReports = new List<APIReportItem>();
        ConcurrentBag<APIReportItem> concurrentUpdatedReports = new();
        ConcurrentQueue<Exception> exceptions = new();

        var alreadyProcessedReports = reportsList
            .Where(report => string.IsNullOrEmpty(report.ReportDocumentId) || report.IsDownloaded)
            .ToList();

        foreach (var report in alreadyProcessedReports)
        {
            report.IsDownloaded = false;
            concurrentUpdatedReports.Add(report);
        }

        // Process only the remaining reports
        var reportsToProcess = reportsList.Except(alreadyProcessedReports).ToList();

        ThrottleCalls(reportsToProcess, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
        {
            Parallel.ForEach(reports, _apiParallelOptions, (report, state) =>
            {
                try
                {
                    var backOffStrategy = new MultiplicativeBackOffStrategy { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);

                    var reportStatusResponse = _amazonSellingPartnerApiService.CheckReportStatusAndDownload(
                        report, cancellableRetry.Execute, queueItem, CurrentIntegration.IntegrationName, _chunkSize);

                    if (reportStatusResponse.reportStatus == InternalReportStatus.COMPLETED.ToString())
                    {
                        report.FileItem = reportStatusResponse.fileCollection;
                        report.IsDownloaded = true;
                        concurrentUpdatedReports.Add(report);
                    }
                    else if (reportStatusResponse.reportStatus == ReportStatus.FATAL.ToString()
                            || reportStatusResponse.reportStatus == InternalReportStatus.FATAL_DUE_TO_UNAVAILABLE_DATA.ToString())
                    {
                        if (report.ReportName.Contains("BrandAnalyticsRepeatPurchaseReport"))
                        {
                            report.ReportId = string.Empty;
                            report.ReportDocumentId = string.Empty;
                            report.IsDownloaded = false;
                            concurrentUpdatedReports.Add(report);
                        }
                        LogMessage(LogLevel.Info, $"{reportStatusResponse.reportStatus} for the report Name: {report.ReportName}" +
                            $", MarketplaceId: {report.MarketplaceId}, APIReportID: {report.APIReportID}" +
                            $", QueueId: {report.FileGuid}. " +
                            $"Report data will not be available in S3 bucket for the scheduled dates.");
                    }
                    else
                    {
                        report.IsDownloaded = false;
                        concurrentUpdatedReports.Add(report);
                        LogMessage(LogLevel.Info, $"Report status is still pending. Report Name: {report.ReportName}" +
                            $", MarketplaceId: {report.MarketplaceId}, APIReportID: {report.APIReportID}" +
                            $", QueueId: {report.FileGuid}");
                    }
                }
                catch (HttpClientProviderRequestException ex)
                {
                    LogException(LogLevel.Error, $"|Exception details : {ex}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
                catch (Exception ex)
                {
                    LogException(LogLevel.Error, $"{ex.Message}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
            });
        });

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        // Convert back to list after parallel execution
        updatedReports = concurrentUpdatedReports.ToList();

        //Delete if all unfinished reports are completed in S3
        if (updatedReports.All(x => x.IsDownloaded))
        {
            //Save report list in S3
            SaveReportsList(queueItem, updatedReports);

            //If all the unfished reports are downloaded then we can delete that unfinished file
            MarkQueueAsComplete(queueItem, updatedReports);
        }
        else
        {
            SaveUnfinishedReports(updatedReports);
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
            LogMessage(LogLevel.Info, $"Reports not ready for queue ID={queueItem.ID}. Reports saved for next run. Queue reset to pending");
        }
    }

    private void SaveReportsList(OrderedQueue queueItem, List<APIReportItem> unfinishedReports)
    {
        var reportList = unfinishedReports.Select(e => new ReportsList
        {
            ReportName = e.ReportName.ToLower(),
            FileDate = e.FileDate
        }).ToList();

        // Convert to JSON
        string jsonString = JsonConvert.SerializeObject(reportList, Formatting.Indented);

        // Write JSON to a MemoryStream
        using (var memoryStream = new MemoryStream())
        {
            var writer = new StreamWriter(memoryStream);
            writer.WriteAsync(jsonString);
            writer.FlushAsync();
            memoryStream.Position = 0; // Reset stream position to the beginning

            var incomingFile = new StreamFile(memoryStream, GreenhouseS3Creds);
            string fileName = queueItem.FileGUID + "_" + "reportslist.json";
            var path = GetS3PathHelper(queueItem.EntityID, queueItem.FileDate, fileName);
            var reportFile = new S3File(new Uri(path), GreenhouseS3Creds);
            UploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
        }
    }

    private void CleanupReports()
    {
        var activeGuids = JobService.GetQueueGuidBySource(CurrentSource.SourceID);

        //Remove any unfinished report files whose queues were deleted
        _unfinishedReportProvider.CleanupReports(_baseDestUri, activeGuids);
    }

    private void MarkQueueAsComplete(OrderedQueue queueItem, IEnumerable<APIReportItem> queueReportList)
    {
        LogMessage(LogLevel.Debug,
            $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; " +
            $"file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}");

        var files = queueReportList.Where(x => x.FileItem != null).Select(x => x.FileItem).ToList();
        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
        queueItem.FileSize = files.Sum(x => x.FileSize);
        queueItem.DeliveryFileDate = queueReportList.Max(x => x.FileDate);

        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update<Queue>((Queue)queueItem);
        _unfinishedReportProvider.DeleteReport(queueItem.FileGUID.ToString());
    }

    private static void ThrottleCalls<T>(IEnumerable<T> source, int nbItemsPerMinute, Action<string> logInfo, Action<IEnumerable<T>> action)
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

    public void PostExecute()
    {

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
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}

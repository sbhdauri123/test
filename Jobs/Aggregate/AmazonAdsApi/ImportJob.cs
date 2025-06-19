using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.AmazonAdsApi;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.Data.DataSource.AmazonAdsApi;
using Greenhouse.Data.DataSource.AmazonAdsApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
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

namespace Greenhouse.Jobs.Aggregate.AmazonAdsApi;

[Export("AmazonAdsApi-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private readonly APIEntityRepository _apiEntityRepository = new APIEntityRepository();
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    List<string> _advertiserIds;

    private AmazonAdsApiOAuth _amazonAdsOAuth;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private AmazonAdsApiService _amazonAdsApiService;
    private List<OrderedQueue> _queueItems;
    private TimeSpan _maxRuntime;
    private int _maxRetry;
    private int _counter;
    private int _exceptionCount;
    private int _startIndex;
    private int _count;
    private int _seed;
    private int _chunkSize;
    private UnfinishedReportProvider<APIReportItem> _unfinishedReportProvider;
    private Uri _baseDestUri;
    private IHttpClientProvider _httpClientProvider;
    private List<ProfileResponse> _profilesList;
    private readonly Stopwatch _runTime = new();

    private ParallelOptions _apiParallelOptions;
    private int _maxAPIRequestPerMinute;
    private int _maxDegreeOfParallelism;
    private string _reportFailureReason;
    private int _advertisersBatchSize;
    private int _nbTopResult;
    private List<long> _failedQueueIDList;
    private bool HasRuntimeExceeded() => TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1;

    public void PreExecute()
    {
        _httpClientProvider = base.HttpClientProvider;

        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();

        LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_MAX_RUNTIME, new TimeSpan(0, 1, 0, 0));
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_POLLY_MAX_RETRY, 10);
        _counter = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_POLLY_COUNTER, 3);
        _startIndex = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_ADVERTISER_START_INDEX, 3);
        _count = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_ADVERTISER_COUNT, 3);
        _seed = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_BACKOFF_DELAY_SECONDS, 4);
        _chunkSize = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_UPLOAD_REPORT_CHUNK_SIZE, 20);
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_MAX_DEGREE_PARALLELISM, 1);
        _maxAPIRequestPerMinute = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_REQUESTS_PER_MINUTE, 120);
        _apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism };
        _reportFailureReason = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_REPORT_FAILURE_REASON, "Report generation failed due to an internal error. Please retry");
        _advertisersBatchSize = LookupService.GetLookupValueWithDefault(Constants.AMAZON_ADS_API_ADVERTISERS_BATCH_SIZE, 5);

        _nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _unfinishedReportProvider = new UnfinishedReportProvider<APIReportItem>(_baseDestUri, LogMessage, LogException);
        CleanupReports();
        var amazonAdsApiServiceArguments = new AmazonAdsApiServiceArguments(base.HttpClientProvider
                                                , CurrentCredential, GreenhouseS3Creds, CurrentIntegration
                                                , GetS3PathHelper, _apiReports
                                                , UploadToS3, LogMessage, LogException);
        _amazonAdsApiService = new AmazonAdsApiService(amazonAdsApiServiceArguments);

        _amazonAdsOAuth = new AmazonAdsApiOAuth(CurrentCredential, LogMessage, LogException, _httpClientProvider);

        _profilesList = _amazonAdsApiService.GetProfilesDataAsync(_amazonAdsOAuth).GetAwaiter().GetResult();
        InsertProfilesData(_profilesList, base.SourceId);

        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, _nbTopResult
                                           , this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)
                                           .OrderBy(q => q.RowNumber).ToList();
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

        ProcessQueueItems();
        CheckReportStatusAndDownload();

        LogMessage(LogLevel.Info, $"{nameof(AmazonAdsApiService)} finalized.");

        if (HasRuntimeExceeded())
        {
            LogRuntimeExceededWarning();
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = "Unfinished reports. Some reports were not ready during this Import and will be picked up by the next one.";
        }

        _runTime.Stop();
        LogMessage(LogLevel.Info, $"Import job completed {nameof(AmazonAdsApiService)}. Took {_runTime.Elapsed}ms to finish.");

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
    }

    private void CheckReportStatusAndDownload()
    {
        foreach (var queueItem in _queueItems)
        {
            try
            {
                if (HasRuntimeExceeded())
                {
                    LogRuntimeExceededWarning();
                    JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                    break;
                }
                if (_failedQueueIDList.Contains(queueItem.ID))
                {
                    LogMessage(LogLevel.Info, $"Skipping download for failed queue: {queueItem.ID}");
                    continue;
                }

                List<APIReportItem> unfinishedForQueue = _unfinishedReportProvider.GetReports(queueItem.FileGUID.ToString())?.ToList() ?? new List<APIReportItem>();
                if (unfinishedForQueue.Count > 0)
                {
                    CheckReportStatusAndDownload(unfinishedForQueue, queueItem);
                }
                else
                {
                    LogMessage(LogLevel.Info, $"There are no newly created and unfinished reports available.");
                    MarkQueueAsComplete(queueItem, unfinishedForQueue);
                }
            }
            catch (Exception ex)
            {
                _exceptionCount++;
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                LogException(LogLevel.Error, $"Failed to download reports for queue: {queueItem.ID}, GUID: {queueItem.FileGUID} and  Exception: {ex.Message}", ex);
            }
        }
    }

    private void ProcessQueueItems()
    {
        _failedQueueIDList = new();
        foreach (var queueItem in _queueItems)
        {
            try
            {
                if (HasRuntimeExceeded())
                {
                    LogRuntimeExceededWarning();
                    JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                    break;
                }

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                LogMessage(LogLevel.Info, $"{nameof(AmazonAdsApiService)} initialized.");
                // Check if the reports have already been submitted (unfinished)
                List<APIReportItem> unfinishedForQueue = _unfinishedReportProvider.GetReports(queueItem.FileGUID.ToString())?.ToList() ?? new List<APIReportItem>();
                if (unfinishedForQueue?.Count == 0)
                {
                    // no unfinish data for the queue, this is the begining of the process
                    // let's clean any old data in the S3 raw folder
                    DeleteRawFiles(queueItem);
                }

                //Get AccountInfo.Type based queue entityID
                string accountType = _profilesList.Where(p => p.ProfileId == queueItem.EntityID)
                                                        .Select(p => p.AccountInfo?.Type)
                                                        .FirstOrDefault();
                if (string.IsNullOrEmpty(accountType))
                {
                    LogMessage(LogLevel.Error, $"Account type is not available for the Entity: {queueItem.EntityID}. Updating Queue as Error.");
                    _failedQueueIDList.Add(queueItem.ID);
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    continue;
                }

                _advertiserIds = new List<string>();

                if (accountType.Equals(AccountType.Agency.ToString(), StringComparison.OrdinalIgnoreCase))
                {
                    _advertiserIds = GetAdvertiserIdsForDSP(queueItem);
                }


                List<APIReportItem> reportsList = new(); //Newly created reports
                ConcurrentBag<APIReportItem> concurrentReportsList = new();
                ConcurrentQueue<Exception> exceptions = new();

                foreach (var report in unfinishedForQueue)
                {
                    concurrentReportsList.Add(report);
                }

                // Process only new reports that are not already created
                var remainingReports = _apiReports.Where(apiReport => !unfinishedForQueue.Any(x => x.APIReportID == apiReport.APIReportID)).ToList();

                List<APIReport<ReportSettings>> reportsToProcess = new();
                foreach (var apiReport in remainingReports)
                {
                    if (accountType.Equals(AccountType.Agency.ToString(), StringComparison.OrdinalIgnoreCase)
                         && apiReport.APIReportName.Contains("AmazonDSP"))
                    {
                        reportsToProcess.Add(apiReport);
                    }
                    else if ((accountType.Equals(AccountType.Vendor.ToString(), StringComparison.OrdinalIgnoreCase)
                                || accountType.Equals(AccountType.Seller.ToString(), StringComparison.OrdinalIgnoreCase))
                                && !apiReport.APIReportName.Contains("AmazonDSP"))
                    {
                        reportsToProcess.Add(apiReport);
                    }
                }

                ThrottleCalls(reportsToProcess, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
                {
                    Parallel.ForEach(reports, _apiParallelOptions, (apiReport, state) =>
                    {
                        try
                        {
                            var backOffStrategy = new MultiplicativeBackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
                            var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);
                            List<APIReportItem> newReportList = CallCreateReport(apiReport, queueItem, cancellableRetry.Execute, accountType);

                            foreach (APIReportItem apiReportItem in newReportList)
                            {
                                concurrentReportsList.Add(apiReportItem);
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

                // Convert back to List after parallel execution
                reportsList = concurrentReportsList.ToList();
                SaveUnfinishedReports(reportsList);

                if (!exceptions.IsEmpty)
                {
                    ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
                }
            }
            catch (Exception ex)
            {
                _exceptionCount++;
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                _failedQueueIDList.Add(queueItem.ID);
                LogException(LogLevel.Error, $"Failed to request report for queue: {queueItem.ID}, GUID: {queueItem.FileGUID} and  Exception: {ex.Message}", ex);
            }
        }
    }

    private List<string> GetAdvertiserIdsForDSP(OrderedQueue queueItem)
    {
        var backOffStrategy = new MultiplicativeBackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
        var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);

        AdvertiserResponse dspAdvertiser = new AdvertiserResponse();
        if (_advertiserIds.Count == 0) //Making sure to call the Get Advertisers API only once for single entity
        {
            while (true)
            {
                string index = $"?startIndex={_startIndex}&count={_count}";

                // Get the advertiserResponse using the current index query string
                dspAdvertiser = _amazonAdsApiService.GetAdvertiserDataAsync(_amazonAdsOAuth
                                                            , queueItem, cancellableRetry.Execute
                                                            , index).GetAwaiter().GetResult();

                // If the advertiserResponse is empty, break the loop
                if (dspAdvertiser.Response != null)
                {
                    if (dspAdvertiser.Response.Count > 0 || dspAdvertiser.Response.Count < 100)
                    {
                        // Collect remaining EmployeeIds if any
                        _advertiserIds.AddRange(dspAdvertiser.Response.Select(r => r.AdvertiserId));
                        break;
                    }
                    _advertiserIds.AddRange(dspAdvertiser.Response.Select(r => r.AdvertiserId));

                    // Update 'startIndex' and double the 'count' for the next request
                    _startIndex += _count + 1;
                    _count += 100;
                }
            }
        }
        return _advertiserIds;
    }

    private void LogRuntimeExceededWarning()
    {
        LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
    }

    public void SaveUnfinishedReports(List<APIReportItem> listAPIReportItem)
    {
        if (listAPIReportItem.Count > 0)
        {
            _unfinishedReportProvider.SaveReport(listAPIReportItem.FirstOrDefault().FileGuid.ToString(), listAPIReportItem);
        }
    }

    private void InsertProfilesData(List<ProfileResponse> profilesList, int sourceId)
    {
        int integrationId = CurrentIntegration.IntegrationID;
        IEnumerable<APIEntity> oldProfilesList = JobService.GetAllAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID);
        //To remove duplicate entities
        IEnumerable<ProfileResponse> newProfilesList = profilesList
                                                       .Where(e => !oldProfilesList
                                                       .Any(ex =>
                                                                string.Equals(ex.APIEntityCode, e.ProfileId
                                                                , StringComparison.OrdinalIgnoreCase)));
        if (newProfilesList.Any())
        {
            List<APIEntity> listEntities = BuildApiEntityObject(newProfilesList, integrationId, sourceId);
            _apiEntityRepository.BulkInsert(listEntities, "ApiEntity");
        }
    }

    private static List<APIEntity> BuildApiEntityObject(IEnumerable<ProfileResponse> profilesList
                                                            , int integrationId, int sourceId)
    {
        List<APIEntity> apiEntityList = new List<APIEntity>();
        foreach (ProfileResponse profile in profilesList)
        {
            APIEntity apiEntity = new APIEntity();
            apiEntity.APIEntityCode = profile.ProfileId;
            apiEntity.APIEntityName = profile.AccountInfo.Name;
            apiEntity.SourceID = sourceId;
            apiEntity.StartDate = DateTime.Today;
            apiEntity.IsActive = true;
            apiEntity.TimeZone = profile.Timezone;
            apiEntity.IntegrationID = integrationId;
            apiEntityList.Add(apiEntity);
        }
        return apiEntityList;
    }
    private List<APIReportItem> CallCreateReport(APIReport<ReportSettings> apiReport, OrderedQueue queueItem, Action<Action> cancellableRetry
                            , string accountType)
    {
        int batchId = 1;
        List<APIReportItem> newReportList = new();
        string jsonPayload = string.Empty;

        if (accountType.Equals(AccountType.Agency.ToString(), StringComparison.OrdinalIgnoreCase)
            && apiReport.APIReportName.Contains("AmazonDSP"))
        {
            //Creating reports for accountype Agency
            var batches = _advertiserIds.Chunk(_advertisersBatchSize);
            foreach (var batch in batches)
            {
                jsonPayload = ApiReportRequest.PrepareJsonObject(queueItem, batch.ToList(), apiReport);
                APIReportItem newReport = CreateReportForQueues(apiReport, queueItem, cancellableRetry, jsonPayload, batchId);
                newReport.AdvertiserIds = batch.ToList();
                newReportList.Add(newReport);
                batchId++;
            }
            LogMessage(LogLevel.Info, $"Completed create report api call for report Name:  {apiReport.APIReportName}" +
                   $", APIReportID:  {apiReport.APIReportID}, Source Id:  {apiReport.SourceID}, QueueId : {queueItem.ID}" +
                   $", AccountType:  {accountType} " +
                   $", Total no of batchIds:  {batchId - 1}, Batchsize : {_advertisersBatchSize} ");
        }
        else if ((accountType.Equals(AccountType.Vendor.ToString(), StringComparison.OrdinalIgnoreCase)
                || accountType.Equals(AccountType.Seller.ToString(), StringComparison.OrdinalIgnoreCase))
                && !apiReport.APIReportName.Contains("AmazonDSP"))
        {
            //Creating reports for accountype Vendor
            jsonPayload = ApiReportRequest.PrepareJsonObject(queueItem, _advertiserIds, apiReport);
            APIReportItem newReport = CreateReportForQueues(apiReport, queueItem, cancellableRetry, jsonPayload, batchId);
            newReportList.Add(newReport);

            LogMessage(LogLevel.Info, $"Completed create report api call for report Name:  {apiReport.APIReportName}" +
                   $", APIReportID:  {apiReport.APIReportID}, Source Id:  {apiReport.SourceID}, QueueId : {queueItem.ID}" +
                   $", AccountType:  {accountType} ");
        }
        return newReportList;
    }

    private APIReportItem CreateReportForQueues(APIReport<ReportSettings> apiReport, OrderedQueue queueItem
                                            , Action<Action> cancellableRetry, string jsonPayload, int batchId)
    {
        string newReportResponse = _amazonAdsApiService.MakeCreateReportApiCallAsync(_amazonAdsOAuth, jsonPayload
                                                                                    , queueItem, apiReport
                                                                                    , cancellableRetry).GetAwaiter().GetResult();
        return BuildAPIReportItemObject(newReportResponse, queueItem, apiReport, batchId);
    }

    private static APIReportItem BuildAPIReportItemObject(string newReportResponse, OrderedQueue queueItem
                                                            , APIReport<ReportSettings> apiReport, int batchId)
    {
        APIReportItem apiReportItem = new APIReportItem()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ProfileID = queueItem.EntityID,
            IsReady = false,
            IsDownloaded = false,
            FileExtension = ".json",
            FileDate = queueItem.FileDate,
            APIReportID = apiReport.APIReportID,
            TaskRunDate = DateTime.Now,
            ReportStartDate = queueItem.FileDate.ToString("yyyy-MM-dd"),
            ReportEndDate = queueItem.FileDate.ToString("yyyy-MM-dd"),
            BatchId = batchId
        };
        if (!string.IsNullOrEmpty(newReportResponse))
        {
            CreateReportResponse newReport = JsonConvert.DeserializeObject<CreateReportResponse>(newReportResponse);
            apiReportItem.ReportId = newReport.ReportId;
            apiReportItem.ReportName = newReport.Name;
            apiReportItem.Status = newReport.Status;
            apiReportItem.ReportURL = newReport.Url;
            apiReportItem.FileName = newReport.Name;
            apiReportItem.FileSize = newReport.FileSize;
        }
        return apiReportItem;
    }

    private void CheckReportStatusAndDownload(List<APIReportItem> newReportsList, OrderedQueue queueItem)
    {
        List<APIReportItem> unfinishedReports = new List<APIReportItem>();

        // Use ConcurrentBag for thread-safe parallel writes
        ConcurrentBag<APIReportItem> concurrentUnfinishedReports = new();
        ConcurrentBag<APIReportItem> concurrentInternalErrorReports = new();
        ConcurrentQueue<Exception> exceptions = new();

        var reportsToSkip = newReportsList
            .Where(newReport => newReport.IsDownloaded || string.IsNullOrEmpty(newReport.ReportId))
            .ToList();

        foreach (var skippedReport in reportsToSkip)
        {
            concurrentUnfinishedReports.Add(skippedReport);
        }

        var reportsToProcess = newReportsList.Where(newReport => !reportsToSkip.Contains(newReport)).ToList();

        ThrottleCalls(reportsToProcess, _maxAPIRequestPerMinute, msg => LogMessage(LogLevel.Info, msg), (reports) =>
        {
            Parallel.ForEach(reports, _apiParallelOptions, (newReport, state) =>
            {
                try
                {
                    var backOffStrategy = new MultiplicativeBackOffStrategy() { Counter = _counter, MaxRetry = _maxRetry, Seed = _seed };
                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), backOffStrategy, _runTime, _maxRuntime);

                    var reportStatusResponse = _amazonAdsApiService.CheckReportStatusAndDownload(
                        _amazonAdsOAuth, newReport, cancellableRetry.Execute, queueItem, _chunkSize, _reportFailureReason);

                    if (reportStatusResponse.reportStatus == ReportStatus.COMPLETED.ToString())
                    {
                        newReport.FileItem = reportStatusResponse.fileCollection;
                        newReport.IsDownloaded = true;
                        concurrentUnfinishedReports.Add(newReport);
                    }
                    else if (reportStatusResponse.reportStatus == InternalReportStatus.INTERNAL_ERROR.ToString())
                    {
                        LogMessage(LogLevel.Info, $"{reportStatusResponse.reportStatus} for the report Name: {newReport.ReportName}" +
                        $", ProfileId : {newReport.ProfileID}, APIReportID: {newReport.ReportId}" +
                        $", QueueId: {newReport.FileGuid}. " +
                            $"Report data will not be available in S3 bucket for the scheduled dates.");
                        concurrentInternalErrorReports.Add(newReport);
                    }
                    else
                    {
                        newReport.IsDownloaded = false;
                        concurrentUnfinishedReports.Add(newReport);
                        LogMessage(LogLevel.Info, $"Report status is still pending. Report Name: {newReport.ReportName}" +
                            $", ProfileId: {newReport.ProfileID}, APIReportID: {newReport.APIReportID}" +
                            $", QueueId: {newReport.FileGuid}");
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

        // Convert ConcurrentBag back to a List after parallel execution
        unfinishedReports = concurrentUnfinishedReports.ToList();

        var internalErrorIds = concurrentInternalErrorReports != null
    ? new HashSet<int>(concurrentInternalErrorReports.Select(x => x.APIReportID))
    : new HashSet<int>(); // Empty HashSet if the source is null

        var reportsWithInternalError = unfinishedReports
            .Where(x => internalErrorIds.Contains(x.APIReportID))
            .ToList();

        if (reportsWithInternalError?.Count > 0)
        {
            LogMessage(LogLevel.Info, $"Reports with internal error found. Reports saved for next run.");
            unfinishedReports = unfinishedReports.Except(reportsWithInternalError).ToList();
            SaveUnfinishedReports(unfinishedReports);
            return;
        }

        if (!exceptions.IsEmpty)
        {
            SaveUnfinishedReports(unfinishedReports);
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }
        if (unfinishedReports.All(x => x.IsDownloaded) && unfinishedReports.Count > 0)
        {
            //Save report list in S3
            SaveReportsList(queueItem, unfinishedReports);

            //If all the unfinished reports are downloaded then we can delete that unfinished file
            MarkQueueAsComplete(queueItem, unfinishedReports);
        }
        else
        {
            SaveUnfinishedReports(unfinishedReports);
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
            LogMessage(LogLevel.Warn, $"Reports not ready for queue ID={queueItem.ID}. Reports saved for next run. Queue reset to pending");
        }
    }

    private void SaveReportsList(OrderedQueue queueItem, List<APIReportItem> unfinishedReports)
    {
        Func<string, DateTime, string, string> getS3PathHelper = GetS3PathHelper;
        Action<IFile, S3File, string[], long, bool> uploadToS3 = UploadToS3;
        Credential greenhouseS3Credential = GreenhouseS3Creds;

        var reportList = unfinishedReports
                            .GroupBy(e => new { e.ReportName, e.FileDate })
                            .Select(g => g.First())
                            .Select(e => new ReportsList
                            {
                                ReportName = e.ReportName,
                                FileDate = e.FileDate
                            })
                            .ToList();

        // Convert to JSON
        string jsonString = JsonConvert.SerializeObject(reportList, Formatting.Indented);

        // Write JSON to a MemoryStream
        using (var memoryStream = new MemoryStream())
        {
            var writer = new StreamWriter(memoryStream);
            writer.WriteAsync(jsonString);
            writer.FlushAsync();
            memoryStream.Position = 0; // Reset stream position to the beginning

            var incomingFile = new StreamFile(memoryStream, greenhouseS3Credential);
            string fileName = queueItem.FileGUID + "_" + "reportslist.json";

            var path = getS3PathHelper(queueItem.EntityID, queueItem.FileDate, fileName);
            var reportFile = new S3File(new Uri(path), greenhouseS3Credential);
            uploadToS3(incomingFile, reportFile, [reportFile.FullName], 0, false);
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
        queueItem.FileCollectionJSON = JsonConvert.SerializeObject(files);
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

using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Facebook;
using Greenhouse.DAL.DataSource.Facebook.Orchestration;
using Greenhouse.Data.DataSource.Facebook;
using Greenhouse.Data.DataSource.Facebook.Core;
using Greenhouse.Data.DataSource.Facebook.GraphApi.Core;
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
using System.Net;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.Facebook;

[Export("Facebook-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private RemoteAccessClient _rac;
    private Uri _baseStageDestUri;
    private Uri _baseRawDestUri;
    private IOrderedEnumerable<OrderedQueue> _queueItems;
    private JsonSerializer _defaultSerializer;
    private Credential _credential;
    private Uri _baseLocalImportUri;
    private int _maxDownloadAttempt;
    private JsonSerializerSettings _redshiftSerializerSettings;
    private JsonSerializer _redshiftSerializer;
    private EntityETLMapRepository _entityETLMapRepository;
    private List<string> _facebookPageRetryList;
    private List<ErrorSignal> _facebookErrorList;
    private ConcurrentBag<ReportState> _facebookReportState;
    private string _facebookReportStateName;
    private ReportState _currentReportState;
    private DateTime _importDateTime;
    private readonly Stopwatch runtime = new Stopwatch();
    private TimeSpan maxRuntime;
    private FacebookBackoff backoffDetailsSummary;
    private List<string> _throttleCodeList;
    private string graphEndpoint;
    private AggregateInitializeSettings aggregateInitializeSettings;
    private int dailyLookback;
    private int bufferDays;
    private int exceptionCounter;
    private int warningCounter;
    private int _maxDegreeOfParallelism;


    // by using a static property, all instances ( = all integrations) will share a reference to the same object
    // using a lock on that object means that only 1 instance at a time can execute the code within the lock
    private static readonly object _classLock = new object();
    private readonly QueueServiceThreadLock _queueServiceThreadLock = new QueueServiceThreadLock(_classLock);


    private string JobGUID
    {
        get { return this.JED.JobGUID.ToString(); }
    }
    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _rac = GetS3RemoteAccessClient();
        _baseRawDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));

        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);

        var integrationID = CurrentIntegration.ParentIntegrationID ?? CurrentIntegration.IntegrationID;
        var queueItems = _queueServiceThreadLock.GetOrderedTopQueueItemsByCredential(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.CredentialID, integrationID)?.ToList();
        _queueItems = queueItems.Select(x => (OrderedQueue)x).OrderBy(x => x.RowNumber);

        _credential = CurrentCredential;
        _defaultSerializer = JsonSerializer.Create(new JsonSerializerSettings() { Formatting = Formatting.None });
        _baseLocalImportUri = GetLocalImportDestinationFolder();
        _maxDownloadAttempt = int.Parse(SetupService.GetById<Lookup>(Common.Constants.FACEBOOK_DIMENSION_BACKOFF_MAX_RETRY).Value);
        _redshiftSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.None };
        _redshiftSerializerSettings.Converters.Add(new Greenhouse.Utilities.IO.RedshiftConverter());
        _redshiftSerializer = JsonSerializer.Create(_redshiftSerializerSettings);
        _entityETLMapRepository = new EntityETLMapRepository();
        var facebookLookupPageRetry = SetupService.GetById<Lookup>(Common.Constants.FACEBOOK_PAGE_RETRY);
        _facebookPageRetryList = string.IsNullOrEmpty(facebookLookupPageRetry?.Value) ? new List<string>() : (facebookLookupPageRetry.Value).Split(',').ToList();
        var facebookLookupError = SetupService.GetById<Lookup>(Constants.FACEBOOK_ERROR);
        _facebookErrorList = string.IsNullOrEmpty(facebookLookupError?.Value) ? new List<ErrorSignal>() : ETLProvider.DeserializeType<List<ErrorSignal>>(facebookLookupError.Value);
        _facebookReportStateName = $"{Constants.FACEBOOK_STATE}_{CurrentIntegration.IntegrationID}";
        var facebookLookupState = SetupService.GetById<Lookup>(_facebookReportStateName);
        _facebookReportState = string.IsNullOrEmpty(facebookLookupState?.Value) ? new ConcurrentBag<ReportState>() : ETLProvider.DeserializeType<ConcurrentBag<ReportState>>(facebookLookupState.Value);

        if (!TimeSpan.TryParse(SetupService.GetById<Lookup>(Constants.FACEBOOK_MAX_RUNTIME)?.Value, out maxRuntime))
        {
            maxRuntime = new TimeSpan(0, 3, 0, 0);
        }

        // the GraphApiInsights class can be the future home of the api operations contained in this import job class
        graphEndpoint = $"{_credential.CredentialSet.Endpoint.TrimEnd('/')}";

        // guides the backoff policy for retrieving and filtering ad ID list
        string backoffValueSummary = SetupService.GetById<Lookup>(Constants.FACEBOOK_BACKOFF_DETAILS_SUMMARY)?.Value;
        backoffDetailsSummary = backoffValueSummary == null ? new FacebookBackoff() { Counter = 0, MaxRetry = 5 } : Newtonsoft.Json.JsonConvert.DeserializeObject<FacebookBackoff>(backoffValueSummary);

        // throttle error codes
        var facebookThrottleCodes = SetupService.GetById<Lookup>(Common.Constants.FACEBOOK_THROTTLE_CODES);
        _throttleCodeList = string.IsNullOrEmpty(facebookThrottleCodes?.Value) ? new List<string>() { "80000", "4" } : (facebookThrottleCodes.Value).Split(',').ToList();
        aggregateInitializeSettings = ETLProvider.DeserializeType<AggregateInitializeSettings>(CurrentSource.AggregateInitializeSettings);
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.FACEBOOK_DAILY_LOOKBACK)?.Value, out dailyLookback))
            dailyLookback = 0;
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.FACEBOOK_CAMPAIGN_FLIGHT_BUFFER)?.Value, out bufferDays))
            bufferDays = 0;
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.FACEBOOK_MAX_DEGREE_PARALLELISM, 4);
    }

    public void Execute()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE START {base.DefaultJobCacheKey}")));

        runtime.Start();
        _importDateTime = DateTime.UtcNow;

        var reports = _entityETLMapRepository.GetEntityAPIReports<FacebookReportSettings>(CurrentSource.SourceID);

        if (reports.Any())
        {
            if (_queueItems.Any())
            {
                var queueByEntity = _queueItems.GroupBy(grp => grp.EntityID);

                Parallel.ForEach(queueByEntity, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, queues =>
                {
                    ImportQueues(reports, queues);
                });

                runtime.Stop();
            }
            else
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("There are no reports in the Queue")));
            }
        }
        else
        {
            var missingReportException = new APIReportException($"No Reports found - check database configuration.");
            exceptionCounter++;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Import job error ->  Exception: {missingReportException.Message}")));
        }

        if (exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors: {exceptionCounter}; Please check Splunk for more detail.");
        }
        else if (warningCounter > 0)
        {
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = $"Total warnings: {warningCounter}; For full list search for Warnings in splunk";
        }
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("Import job complete")));
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE END {base.DefaultJobCacheKey}")));
    }

    private void ImportQueues(IEnumerable<MappedReportsResponse<FacebookReportSettings>> allApiReports, IGrouping<string, OrderedQueue> queueByEntity)
    {
        // Date Tracker helps find the first queue in daily job based on lookback range
        // (EX: Given last 3 days are 10/23, 10/22 and 10/21. Primary queue would be 10/23)
        // first queue will be the only queue to retrieve data
        // and all other queues with the range will be pending upon first queue completion
        var dateTracker = GetDateTracker();
        if (dateTracker == null)
            throw new APIReportException($"Unable to get Date Tracker--cannot process queue items without it");

        var reportManager = new ReportManager(JobGUID, _rac, _baseStageDestUri, queueByEntity.Key, CurrentIntegration.IntegrationID);

        var snapshotDownloadBackoff = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = 0,
            Seed = 1,
            RandomMin = 1,
            RandomMax = 3
        };

        // load snapshots from S3 that contain pending reports from previous job runs
        var snapshotPolly = base.GetPollyPolicy<Exception>($"FB-{queueByEntity.Key}-LoadSnapshots", snapshotDownloadBackoff);
        reportManager.LoadSnapshots(snapshotPolly);

        // load saved dimension lists from S3 from previous job runs
        var savedVaultPolly = base.GetPollyPolicy<Exception>($"FB-{queueByEntity.Key}-LoadIdVaults", snapshotDownloadBackoff);
        reportManager.LoadSavedIdVault(savedVaultPolly);

        var queueItems = queueByEntity.ToList();

        // get reports where it is either default or entity has opted in
        var reportList = allApiReports.Where(x => x.IsDefault || x.EntityID.Equals(queueByEntity.Key, StringComparison.InvariantCultureIgnoreCase)).ToList();
        if (reportList.Count == 0)
        {
            var totalDefaultReports = allApiReports.Count(x => x.IsDefault);
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Skipping entire queue - No API reports found for entity ({queueByEntity.Key}); total default reports: {totalDefaultReports}")));
            exceptionCounter++;
            return;
        }

        foreach (Queue queueItem in queueItems.OrderBy(q => q.RowNumber))
        {
            var queueItemTimer = new Stopwatch();
            var queueDateTracker = dateTracker.QueueInfoList.Find(x => x.QueueID == queueItem.ID);

            if (queueDateTracker != null)
            {
                // skip queue-item if it is not the primary date in the Date Tracker
                if (!queueDateTracker.IsPrimaryDate)
                {
                    _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"{queueItem.FileGUID}-skipping queue item {queueItem.FileDate:yyyy-MM-dd} because it is within the daily job's lookback window.")));
                    continue;
                }
            }

            if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
            {
                //the runtime is greater than the max RunTime
                var maxRunTimeException = new AllotedRunTimeExceededException($"The runtime ({runtime.Elapsed}) exceeded the allotted time {maxRuntime}");
                exceptionCounter++;
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Import job error ->  Exception: {maxRunTimeException.Message}")));
                break;
            }

            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Running, false);
            queueItemTimer.Start();

            var insightsReportList = new List<FacebookReportItem>();

            var reportIDsReady = QueueReport(queueItem, insightsReportList, queueDateTracker, reportManager, reportList);

            if (insightsReportList.Any(x => x.SkipEntity == true))
            {
                SkipEntity(queueItem, "QueueReport", reportManager);
                break;
            }
            else if (!reportIDsReady) continue;

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"facebookStep=Reports Queued|fileguid={queueItem.FileGUID}|entity={queueItem.EntityID}|filedate={queueItem.FileDate}|totalMinutes={queueItemTimer.Elapsed.TotalMinutes}")));

            // Snapshot # 1
            reportManager.TakeSnapshot(insightsReportList, queueItem);

            var factFilesReady = CheckStatusAndDownloadReport(queueItem, insightsReportList, queueDateTracker, reportManager, reportList);

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"facebookStep=Reports Downloaded|fileguid={queueItem.FileGUID}|entity={queueItem.EntityID}|filedate={queueItem.FileDate}|totalMinutes={queueItemTimer.Elapsed.TotalMinutes}")));

            // log time spent getting insights reports
            LogRecordedTime(queueItem, insightsReportList);

            if (insightsReportList.Any(x => x.SkipEntity == true))
            {
                SkipEntity(queueItem, "CheckStatusAndDownloadReport", reportManager);
                break;
            }
            else if (!factFilesReady) continue;

            StageFacebookFiles(queueItem, queueDateTracker, dateTracker, reportManager);

            queueItemTimer.Stop();
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Queue imported successfully|fileguid={queueItem.FileGUID}|entity={queueItem.EntityID}|filedate={queueItem.FileDate}|totalMinutes={queueItemTimer.Elapsed.TotalMinutes}")));
        }
    }

    /// <summary>
    /// Date Tracker support use of the Graph api's date presets (ex last_3days)
    /// </summary>
    /// <returns></returns>
    private QueueDateTracker GetDateTracker()
    {
        var dateTracker = new QueueDateTracker
        {
            QueueInfoList = new List<QueueInfo>()
        };

        try
        {
            // no need to get queue info if there is no daily lookback
            if (dailyLookback == 0)
                return dateTracker;

            var dailyQueues = _queueItems.Where(x => !x.IsBackfill && x.FileDate >= _importDateTime.AddDays(-dailyLookback).Date);
            if (!dailyQueues.Any())
                return dateTracker;

            // get file item with Primary date (-1 offset)
            var primaryDate = _importDateTime.AddDays(-1).Date;

            // group daily queues by file date
            var dailyGroups = dailyQueues.GroupBy(x => x.FileDate).OrderByDescending(group => group.First().FileDate);

            // add to date-tracker dictionary
            // assign false for dates that are within lookback range
            foreach (var group in dailyGroups)
            {
                var queueList = group.ToList();
                queueList.ForEach(queue =>
                {
                    var queueDateTracker = new QueueInfo()
                    {
                        QueueID = queue.ID,
                        EntityID = queue.EntityID,
                        IsPrimaryDate = queue.FileDate == primaryDate.Date
                    };

                    dateTracker.QueueInfoList.Add(queueDateTracker);
                });
            }

            // possible scenario is the queue (w/ primary date) has already completed
            // and may have failed to update the pending queues within its lookback range
            // Checking here if the primary date is not in current queue list
            var primaryEntities = _queueItems.Where(x => x.FileDate == primaryDate.Date).Select(x => x.EntityID).Distinct();

            // get all Api Entities in queue
            var queueEntities = _queueItems.Select(x => x.EntityID).Distinct();
            var missingPrimaryEntities = queueEntities.Except(primaryEntities);
            if (missingPrimaryEntities.Any())
            {
                var completedEntities = new List<string>();

                var activeEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID);
                var apiEntities = activeEntities.Where(x => missingPrimaryEntities.Contains(x.APIEntityCode));

                // check Lookup FACEBOOK_STATE for last time the daily job completed
                foreach (var apiEntity in apiEntities)
                {
                    var reportState = _facebookReportState.FirstOrDefault(s => s.AccountId.Equals(apiEntity.APIEntityCode, StringComparison.InvariantCultureIgnoreCase));

                    if (reportState?.DailyCompletionDate != null)
                    {
                        if (reportState.DailyCompletionDate.Value.Date == primaryDate.Date)
                        {
                            completedEntities.Add(apiEntity.APIEntityCode);
                            continue;
                        }
                    }

                    dateTracker.QueueInfoList.RemoveAll(x => x.EntityID == apiEntity.APIEntityCode);
                }

                // if the primary date queue is completed, then we mark the pending queues complete
                foreach (var entity in completedEntities)
                {
                    var pendingDates = dateTracker.QueueInfoList.Where(x => x.EntityID == entity && !x.IsPrimaryDate);
                    var queuesToDelete = _queueItems.Where(x => pendingDates.Any(d => d.QueueID == x.ID));
                    if (queuesToDelete.Any())
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"The following queues were marked as import-complete and deleted as 1 queue per entity ({entity}) will contain all reports." +
                            $" Deleting queue IDs={string.Join(", ", queuesToDelete.Select(q => q.ID))}")));
                        UpdateQueueWithDelete(queuesToDelete, Constants.JobStatus.Complete, true);
                    }
                }
            }
        }
        catch (Exception exc)
        {
            exceptionCounter++;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(
                $"Error GetDateTracker -> Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
            return null;
        }
        return dateTracker;
    }

    /// <summary>
    /// Get list of Ad IDs by leveraging the campaign hierarchy; however, if these API reports are unavailable then retrieve directly from account
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="fileGuid"></param>
    /// <param name="queueInfo"></param>
    /// <returns></returns>
    private List<DataAdDimension> GetAdIds(Queue queueItem, string fileGuid, QueueInfo queueInfo, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        var adDimensionList = new List<DataAdDimension>();
        var accountID = queueItem.EntityID;

        // Best practice is to get the Ad list by leveraging the heirarchy
        // ie Campaigns-from-Account => Ad-Sets-from-Campaign => Ads-from-Ad-Sets
        var campaignListReport = reportList.Find(r => r.ReportSettings.ReportType == "list" && r.ReportSettings.Entity == "campaigns" && !r.ReportSettings.IsSummaryReport && r.IsActive);
        var adSetListReport = reportList.Find(r => r.ReportSettings.ReportType == "list" && r.ReportSettings.Entity == "adsets" && !r.ReportSettings.IsSummaryReport && r.IsActive);
        var adListReport = reportList.Find(r => r.ReportSettings.ReportType == "list" && r.ReportSettings.Entity == "ads" && !r.ReportSettings.IsSummaryReport && r.IsActive);

        if (campaignListReport == null | adSetListReport == null | adListReport == null)
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Unable to get list of Ad dimensions due to missing List-Reports. Please ensure there is one Report-Type List for each hierarchy level;" +
                $"entityID:{accountID};guid:{fileGuid}")));
            return null;
        }

        // get Campaigns from Account
        var campaignEntities = GetDimensionList<DataAdCampaignDimension>(accountID, fileGuid, campaignListReport);

        if (campaignEntities == null)
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"Unable to get list of campaignEntities|entityID:{accountID};guid:{fileGuid}")));
            return null;
        }

        var campaignIdList = campaignEntities.ConvertAll(x => x.Id);
        var campaignsWithImpressions = FilterIdList(queueItem, campaignIdList, queueInfo, reportList);

        // get Ad sets from each campaign
        var adsetByCampaign = new Dictionary<string, List<DataAdSetDimension>>();
        foreach (var campaignId in campaignsWithImpressions)
        {
            var adsetsByCampaign = GetDimensionList<DataAdSetDimension>(accountID, fileGuid, adSetListReport, campaignId);
            adsetByCampaign.Add(campaignId, adsetsByCampaign);
        }

        var adsetIdList = adsetByCampaign.SelectMany(x => x.Value.Select(adset => adset.Id)).ToList();
        var adsetsWithImpressions = FilterIdList(queueItem, adsetIdList, queueInfo, reportList);

        var adDimensionByAdset = new List<DataAdDimension>();
        // get Ads from each ad set
        foreach (var adsetId in adsetsWithImpressions)
        {
            var adEntities = GetDimensionList<DataAdDimension>(accountID, fileGuid, adListReport, adsetId);
            adDimensionByAdset.AddRange(adEntities);
        }

        var adIdList = adDimensionByAdset.Select(x => x.AdId).Distinct().ToList();
        var adsWithImpressions = FilterIdList(queueItem, adIdList, queueInfo, reportList);

        if (adsWithImpressions.Count != 0)
        {
            var cachedAdsWithData = adDimensionByAdset.Where(x => adsWithImpressions.Contains(x.AdId));
            adDimensionList.AddRange(cachedAdsWithData);
        }

        return adDimensionList;
    }

    private List<T> GetDimensionList<T>(string accountID, string fileGuid, MappedReportsResponse<FacebookReportSettings> listReport, string entityID = null)
    {
        List<T> dimensionObjects;
        var localTimer = new Stopwatch();
        localTimer.Start();

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Getting {listReport.ReportSettings.Entity} list for Report:{listReport.APIReportName}-{listReport.ReportSettings.Filtering}" +
            $"entityID:{entityID};guid:{fileGuid}")));

        var apiRequest = new GraphApiRequest(_httpClientProvider, graphEndpoint, JobGUID, _credential.CredentialSet.AccessToken, _credential.CredentialSet.Version, accountID, listReport.ReportSettings.Entity, entityID)
        {
            MethodType = System.Net.Http.HttpMethod.Get
        };

        var downloadPolicyStrategy = new BackOffStrategy
        {
            Counter = backoffDetailsSummary.Counter,
            MaxRetry = backoffDetailsSummary.MaxRetry,
            Seed = 1,
            RandomMin = 0,
            RandomMax = 0
        };

        var downloadPolicy = new FacebookRetry(fileGuid, downloadPolicyStrategy, runtime, maxRuntime);

        dimensionObjects = downloadPolicy.Execute(() => apiRequest.GetPagedData<T>(listReport, false));

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{listReport.ReportSettings.Entity} list for Report({entityID}) {listReport.APIReportName} retrieved total dimension objects:{dimensionObjects.Count};TotalMinutes:{localTimer.Elapsed.TotalMinutes}")));
        localTimer.Stop();
        return dimensionObjects;
    }

    /// <summary>
    /// Filter list of Ad IDs to ones with delivery data only
    /// </summary>
    /// <param name="adIdList"></param>
    /// <returns></returns>
    public List<string> FilterIdList(Queue queueItem, List<string> rawList, QueueInfo queueInfo, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        var idList = new List<string>();

        var adIdList = new List<string>();

        if (rawList.Count != 0)
            adIdList.AddRange(rawList);

        var knownStatusCount = 0;
        var unknownIdCount = 0;
        var adsWithDataCount = 0;

        var localTimer = new Stopwatch();
        localTimer.Start();

        // filter ad IDs with delivery data only is the goal here
        // this list-summary api report is designed to check for each Ad ID
        // whether it has insights data
        // if it does not, then we won't make an async call to get data
        var listSummaryReport = reportList.Find
            (r => r.ReportSettings.Level == "ad" && r.ReportSettings.ReportType == "list"
            && r.ReportSettings.IsSummaryReport && r.IsActive);

        if (listSummaryReport != null)
        {
            bool isDailyJob = !queueItem.IsBackfill;
            bool usePreset = CheckFileDate(queueItem, queueInfo);

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Filtering ID list(Total:{adIdList.Count}) for Report:{listSummaryReport.APIReportName}-{listSummaryReport.ReportSettings.Filtering}-" +
                $"AccountID:{queueItem.EntityID};guid:{queueItem.FileGUID.ToString()}")));

            if (adIdList.Count != 0)
            {
                var apiCallsBackOffStrategy = new BackOffStrategy()
                {
                    Counter = backoffDetailsSummary.Counter,
                    MaxRetry = backoffDetailsSummary.MaxRetry
                };

                var pollingPolicy = new CancellableConditionalRetry<bool>(this.JobGUID, apiCallsBackOffStrategy, runtime, maxRuntime, (bool allReportsDone) => allReportsDone == false);

                bool isComplete = false;
                var adIdDictionary = adIdList.Distinct().ToDictionary(x => x, y => false);

                pollingPolicy.Execute(() =>
                {
                    var unfilteredAds = adIdDictionary.Where(x => x.Value == false).Select(x => x.Key).ToList();

                    var graphApiInsights = new GraphApiInsights(_httpClientProvider, graphEndpoint, JobGUID, _credential.CredentialSet.AccessToken, _credential.CredentialSet.Version);
                    var filteredAds = graphApiInsights.FilterObjectIdList(listSummaryReport, unfilteredAds, usePreset, queueItem.FileDate);

                    // update status in dictionary list to avoid re-checking Ad ID
                    var successResponses = filteredAds.Where(x => x.BatchItemResponse.Code == 200).Select(x => x.EntityID).ToList();

                    if (successResponses.Count != 0)
                    {
                        successResponses.ForEach(x => adIdDictionary[x] = true);
                    }

                    var adsWithData = filteredAds.Where(x => x.BatchItemResponse.Code == 200 && x.GetResponseData<StatsReportData>().data.Count != 0)
                        .Select(x => x.EntityID).ToList();

                    if (adsWithData.Count != 0)
                        idList.AddRange(adsWithData);

                    // if any requests error, then we try them again
                    var errorResponses = filteredAds.Where(x => x.BatchItemResponse.Code != 200);

                    if (!errorResponses.Any())
                        isComplete = true;

                    return isComplete;
                });

                adsWithDataCount = idList.Count;
                knownStatusCount = adIdDictionary.Count(x => x.Value == true);

                // adding the ads we are not sure if they have data or not
                // ie the ones we could not complete the previous call for
                var unknownAds = adIdDictionary.Where(x => x.Value == false).Select(x => x.Key).ToList();
                unknownIdCount = unknownAds.Count;

                if (unknownAds.Count != 0)
                {
                    idList.AddRange(unknownAds);
                }
            }
        }

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"guid:{queueItem.FileGUID}-Filtering ID List({queueItem.EntityID}) {listSummaryReport.APIReportName} " +
            $"total ads at start:{adIdList.Count};total with data:{adsWithDataCount};unknown delivery status:{unknownIdCount};Successful calls:{knownStatusCount};total ads returned:{idList.Count};TotalMinutes:{localTimer.Elapsed.TotalMinutes}")));
        localTimer.Stop();

        return idList;
    }

    /// <summary>
    /// For each report type, we track the total minutes, size and times we take any of the following actions:
    /// 1) make graph api calls, 2) run and wait for an async job to complete, 3) download data to local file, and 4) stage data in s3 bucket
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="reportList"></param>
    private void LogRecordedTime(Queue queueItem, List<FacebookReportItem> reportList)
    {
        var reportsByType = reportList.GroupBy(x => x.ReportName);

        foreach (var report in reportsByType)
        {
            Double graphCallsMinutes;
            Double graphAsyncMinutes;

            if (report.ToList().Count > 1)
            {
                // We are batching our calls, so in the case of AdSetStats, we are only waiting as long as the slowest report (ie ad set ID)
                graphCallsMinutes = report.Max(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_BATCH_REQUEST).Sum(z => z.Value.RecordedTime.TotalMinutes));
                graphAsyncMinutes = report.Max(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_ASYNC_REPORT).Sum(z => z.Value.RecordedTime.TotalMinutes));
            }
            else
            {
                // Otherwise we sum the total minutes spent making calls to page through the data
                graphCallsMinutes = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_BATCH_REQUEST).Sum(z => z.Value.RecordedTime.TotalMinutes));
                graphAsyncMinutes = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_ASYNC_REPORT).Sum(z => z.Value.RecordedTime.TotalMinutes));
            }

            var graphCallsCounter = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_BATCH_REQUEST).Sum(z => z.Value.Counter));
            var graphCallsSize = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_BATCH_REQUEST).Sum(z => z.Value.DownloadSize));

            var graphAsyncCounter = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_ASYNC_REPORT).Sum(z => z.Value.Counter));
            var graphAsyncSize = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GRAPH_ASYNC_REPORT).Sum(z => z.Value.DownloadSize));

            var downloadMinutes = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_DOWNLOAD_FILE).Sum(z => z.Value.RecordedTime.TotalMinutes));
            var downloadCounter = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_DOWNLOAD_FILE).Sum(z => z.Value.Counter));
            var downloadSize = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_DOWNLOAD_FILE).Sum(z => z.Value.DownloadSize));

            var stagingMinutes = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_STAGE_FILE).Sum(z => z.Value.RecordedTime.TotalMinutes));
            var stagingCounter = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_STAGE_FILE).Sum(z => z.Value.Counter));
            var stagingSize = report.Sum(x => x.TimeTracker.SessionLog.Where(y => y.Key == SessionTypeEnum.GH_STAGE_FILE).Sum(z => z.Value.DownloadSize));

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{queueItem.FileGUID.ToString()}-RecordedTime=>E:{queueItem.EntityID};D:{queueItem.FileDate:yyyy-MM-dd};" +
                $"ReportType:{report.Key};TotalReports:{report.ToList().Count};" +
                $"GraphRequest=Min:{graphCallsMinutes};Cnt:{graphCallsCounter};Sz:{graphCallsSize};" +
                $"GraphAsync=Min:{graphAsyncMinutes};Cnt:{graphAsyncCounter};Sz:{graphAsyncSize};" +
                $"Download=Min:{downloadMinutes};Cnt:{downloadCounter};Sz:{downloadSize};" +
                $"S3Staging=Min:{stagingMinutes};Cnt:{stagingCounter};Sz:{stagingSize}")));
        }
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
            _rac?.Dispose();
        }
    }
    ~ImportJob()
    {
        Dispose(false);
    }

    private bool DownloadDimensionData(Queue queueItem, List<FacebookReportItem> dimReportList, QueueInfo queueInfo, ReportManager reportManager, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        var isSuccessful = false;
        var isDailyJob = !queueItem.IsBackfill;

        var today = DateTime.UtcNow.Date;

        var accountID = queueItem.EntityID.ToLower();
        if (_facebookReportState.All(s => !s.AccountId.Equals(accountID, StringComparison.InvariantCultureIgnoreCase)))
        {
            _facebookReportState.Add(new ReportState { AccountId = accountID });
        }
        _currentReportState = _facebookReportState.First(s => s.AccountId.Equals(accountID, StringComparison.InvariantCultureIgnoreCase));

        // reportStateDate is last time data was pulled for account level dimensions (examples are Account and Creative)
        // if deltadate is null, then we force the job to get dimension data by setting the report-state-date to a lesser date than now
        var reportStateDate = _currentReportState.DeltaDate == null ? DateTime.UtcNow.AddDays(-2) : _currentReportState.DeltaDate.Value.Date;

        var dimensionReports = reportList.Where(r => r.ReportSettings.ReportType.Equals("dimension", StringComparison.InvariantCultureIgnoreCase) && r.IsActive);

        List<string> adList = new();

        foreach (var dimReport in dimensionReports.OrderBy(d => d.ReportSettings.ReportOrder))
        {
            // retrieve dimension data per ad id
            // other reports will be account basis
            var idList = new List<string>();

            if (!string.IsNullOrEmpty(dimReport.ReportSettings.Level))
            {
                var listAsset = Utilities.UtilsText.ConvertToEnum<ListAsset>(dimReport.ReportSettings.Level);
                idList = reportManager.GetIdsForDimensionDownload(queueItem, listAsset);
                if (idList.Count == 0)
                {
                    continue;
                }
                if (listAsset == ListAsset.Ad)
                {
                    adList.AddRange(idList);
                }
            }
            else if (dimReport.ReportSettings.Entity.Equals("adcreatives", StringComparison.InvariantCultureIgnoreCase))
            {
                if (adList.Count == 0)
                {
                    continue;
                }
                idList = adList;
            }
            else if (_importDateTime.Date == reportStateDate.Date)
            {
                // report state is current today (ie account level dimension data has already been retrieved and downloaded)
                // then skip creating a report item
                continue;
            }

            var reportRequest = new GraphApiRequest(_httpClientProvider, graphEndpoint, JobGUID, _credential.CredentialSet.AccessToken, _credential.CredentialSet.Version, accountID, dimReport.ReportSettings.Entity);
            var usePreset = CheckFileDate(queueItem, queueInfo);
            var reportItems = ReportManager.CreateReportItems(queueItem, dimReport, isDailyJob, true, reportRequest, usePreset, idList);
            dimReportList.AddRange(reportItems);
        }

        if (dimReportList.Count != 0)
        {
            isSuccessful = RetrieveReports(queueItem, dimReportList, reportList);

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid($"All dimension reports are ready for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID}")));
        }
        else
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid("There are no dimension reports to run")));
            isSuccessful = true;
        }
        return isSuccessful;
    }

    private bool RetrieveReports(Queue queueItem, List<FacebookReportItem> dimReportList, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        var allDone = false;
        var skipEntity = false;
        var retryCounter = 0;
        var reportsToDownload = dimReportList.Where(x => !x.IsDownloaded).ToList();
        if (reportsToDownload.Count == 0)
            return false;
        //retry as many times as there are retry options in the lookup value
        //other criteria that will stop execution: 1) no new page sizes to retry and 2) encounter any other fatal errors to skip entity
        do
        {
            try
            {
                if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
                {
                    //the runtime is greater than the max RunTime
                    throw new AllotedRunTimeExceededException($"The runtime ({runtime.Elapsed}) exceeded the allotted time {maxRuntime}");
                }

                var isSuccess = CheckAPIPagingLimit(queueItem, reportsToDownload, reportList, ref retryCounter);

                if (isSuccess)
                {
                    var apiCallsForDimensionBackOffStrategy = new BackOffStrategy { Counter = 0, MaxRetry = backoffDetailsSummary.MaxRetry };
                    var reports = reportsToDownload.Where(x => !x.IsDownloaded).ToList();
                    var httpMethod = System.Net.Http.HttpMethod.Get.ToString();
                    MakeBatchRequest(reports, queueItem, apiCallsForDimensionBackOffStrategy, httpMethod, false, "DownloadDimension", dimReportList);
                }
            }
            catch (Exception exc)
            {
                exceptionCounter++;
                UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Error, deleteQueueItem: false);
                _queueItems.First(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Error;
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(
                    $"Error queueing dimension reports -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
                return false;
            }
            skipEntity = reportsToDownload.Any(x => x.SkipEntity == true);
            allDone = reportsToDownload.All(x => x.IsDownloaded == true) || skipEntity;
        } while (!allDone);

        if (skipEntity)
            return false;

        return allDone;
    }

    /// <summary>
    /// Check if any report items need their pagination limit reduced
    /// due to Facebook API error "Please reduce the amount of data you're asking for, then retry your request"
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="reportList"></param>
    /// <param name="retryCounter"></param>
    /// <param name="isInsightsRequest"></param>
    /// <returns></returns>
    private bool CheckAPIPagingLimit(Queue queueItem, List<FacebookReportItem> reportList, List<MappedReportsResponse<FacebookReportSettings>> apiReportList, ref int retryCounter, bool isInsightsRequest = false)
    {
        var isSuccess = false;
        var retryReports = reportList.Where(x => x.RetryPageSize == true);
        if (retryReports.Any())
        {
            retryCounter++;
            if (_facebookPageRetryList.Count >= retryCounter)
            {
                foreach (var report in retryReports)
                {
                    // remove local file of raw data streamed so far
                    // we need a fresh start when change the page size
                    var localFilePath = GetReportPath(report, queueItem);
                    Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, localFilePath);
                    FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);
                    if (tempDestFile.Exists)
                        tempDestFile.Delete();

                    //get new url with next page size
                    var nextPageSize = FacebookReportItem.GetNextPageSize(_facebookPageRetryList, report.PageSize);
                    if (string.IsNullOrEmpty(nextPageSize))
                    {
                        //skip this entity as there are no new page sizes to retry with
                        report.SkipEntity = true;
                        isSuccess = false;
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid(
                                $"Report:{report.ReportName} EntityStatus:{report.EntityStatus} - Next page size is NOT available - SKIPPING PAGINATION RETRY OF API CALLS" +
                                $"-> current page size: {report.PageSize} for call {report.RelativeUrl}")));
                        break;
                    }

                    if (isInsightsRequest)
                    {
                        report.OriginalInsightsUrl = $"{_credential.CredentialSet.Version}/{report.ReportRunId}/insights/?limit={nextPageSize}";
                        report.RelativeInsightsUrl = $"{_credential.CredentialSet.Version}/{report.ReportRunId}/insights/?limit={nextPageSize}";
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                            PrefixJobGuid($"Report:{report.ReportName} EntityStatus:{report.EntityStatus} - Page size changed from {report.PageSize} to {nextPageSize}" +
                            $"; Relative url: {report.RelativeInsightsUrl}")));
                    }
                    else
                    {
                        var reportType = apiReportList.Find(x => x.APIReportName.Equals(report.ReportName, StringComparison.InvariantCultureIgnoreCase));
                        var reportRequest = new GraphApiRequest(_httpClientProvider, graphEndpoint, JobGUID, _credential.CredentialSet.AccessToken, _credential.CredentialSet.Version, queueItem.EntityID, reportType.ReportSettings.ReportType);
                        reportRequest.SetParameters(reportType, false, report.EntityStatus, nextPageSize);
                        var newUrl = reportRequest.UriPath;
                        report.OriginalUrl = newUrl;
                        report.RelativeUrl = newUrl;
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                            PrefixJobGuid($"Report:{report.ReportName} EntityStatus:{report.EntityStatus} - Page size changed from {report.PageSize} to {nextPageSize}" +
                            $"; Relative url: {report.RelativeUrl}")));
                    }

                    report.PageSize = nextPageSize;
                    report.RetryPageSize = false;
                    isSuccess = true;
                }
            }
            else
            {
                isSuccess = false;
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid($"Exceeded limit of {_facebookPageRetryList.Count} retries - SKIPPING PAGINATION RETRY OF API CALLS")));
            }
        }
        else
        {
            isSuccess = true;
        }
        return isSuccess;
    }

    private void MakeBatchRequest(List<FacebookReportItem> reports, Queue queueItem, IBackOffStrategy apiBackOffStrategy, string httpMethod, bool isInsightsRequest, string requestType
        , List<FacebookReportItem> allReportItems = null)
    {
        var batchResponses = new List<BatchResponse>();
        var batchRequest = new ApiBatchRequest()
        {
            BatchOperations = new List<BatchOperation>()
        };
        var batchCounter = 0;

        for (var i = 0; i < reports.Count; i++)
        {
            if (reports.Any(x => x.SkipEntity) || reports.Any(x => x.RetryPageSize) || reports.Any(x => x.DownloadFailed))
                break;

            var reportItem = reports[i];
            reportItem.IsReady = false;
            reportItem.RetryAttempt++;
            reportItem.TimeTracker.StartSession(SessionTypeEnum.GRAPH_BATCH_REQUEST);

            var batchOperation = new BatchOperation
            {
                method = httpMethod
            };

            if (requestType == "CheckStatus")
            {
                batchOperation.relative_url = $"{_credential.CredentialSet.Version}/{reportItem.ReportRunId}";
            }
            else
            {
                batchOperation.relative_url = isInsightsRequest ? reportItem.RelativeInsightsUrl : reportItem.RelativeUrl;
            }

            batchRequest.ApiVersion = _credential.CredentialSet.Version;
            batchRequest.MethodType = System.Net.Http.HttpMethod.Post;
            batchRequest.BatchOperations.Add(batchOperation);

            batchCounter++;
            if (batchCounter % 50 == 0 || (i == reports.Count - 1))
            {
                var retry = new FacebookRetry(this.JED.JobGUID.ToString(), apiBackOffStrategy, runtime, maxRuntime);

                retry.Execute(() =>
                    SubmitBatchRequestAsync(batchRequest, reports, queueItem, batchResponses, requestType,
                        allReportItems).GetAwaiter().GetResult());

                //re-initialize batch object and batch counter.
                batchRequest = new ApiBatchRequest()
                {
                    BatchOperations = new List<BatchOperation>()
                };

                batchRequest.Index = batchCounter;

                Task.Delay(300).Wait();
            }
        }
    }

    private async Task<bool> SubmitBatchRequestAsync(ApiBatchRequest batchRequest, List<FacebookReportItem> reports, Queue queueItem, List<BatchResponse> batchResponses, string requestType
        , List<FacebookReportItem> allReportItems)
    {
        bool isSuccess = false;
        try
        {
            List<BatchResponse> apiBatchResponses = await _httpClientProvider
                .SendRequestAndDeserializeAsync<List<BatchResponse>>(new HttpRequestOptions
                {
                    Uri = $"{_credential.CredentialSet.Endpoint.TrimEnd('/')}/",
                    Method = HttpMethod.Post,
                    AuthToken = _credential.CredentialSet.AccessToken,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(batchRequest.GetBatchRequestBody(), Encoding.UTF8,
                        MediaTypeNames.Application.Json),
                });

            var batchIndex = batchRequest.Index;
            batchResponses.AddRange(apiBatchResponses);
            var responseList = new List<ApiReportResponse>();
            var logUtilizationWarning = true;

            for (var k = batchIndex; k < batchResponses.Count; k++)
            {
                var responseItem = new ApiReportResponse
                {
                    BatchItemResponse = batchResponses[k],
                    ReportItem = reports[k],
                    ApiError = ETLProvider.DeserializeType<ApiError>(batchResponses[k].Body)
                };

                reports[k].TimeTracker.SaveSession(SessionTypeEnum.GRAPH_BATCH_REQUEST, batchResponses[k].Body.Length);

                // check for errors
                if (_throttleCodeList.Contains(responseItem.ApiError?.Error?.Code.ToString()))
                {
                    throw new FacebookApiThrottleException(PrefixJobGuid($"Api throttle error occurred|code={responseItem.ApiError?.Error?.Code}|message={responseItem.ApiError?.Error?.Message}|body={responseItem.BatchItemResponse.Body}|headers={String.Join(",", responseItem.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}" +
                        $"|Report={responseItem.ReportItem.ReportName}|status:{responseItem.ReportItem.EntityStatus}|url:{responseItem.ReportItem.RelativeUrl}|retryAfterInMinutes={responseItem.RetryAfterInMinutes}"), responseItem.RetryAfterInMinutes);
                }

                // check rate limit headers to see if we are close to having all calls throttled
                if (responseItem.AppUtilizationPercentage > 95 || responseItem.AccountUtilizationPercentage > 95)
                {
                    throw new FacebookApiThrottleException(PrefixJobGuid($"Utilization is above 95 pct|appPct={responseItem.AppUtilizationPercentage}|accountPct={responseItem.AccountUtilizationPercentage}|headers={String.Join(",", responseItem.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}" +
                        $"|Report={responseItem.ReportItem.ReportName}|status:{responseItem.ReportItem.EntityStatus}|url:{responseItem.ReportItem.RelativeUrl}|retryAfterInMinutes={responseItem.RetryAfterInMinutes}"));
                }

                if (logUtilizationWarning && (responseItem.AppUtilizationPercentage > 90 || responseItem.AccountUtilizationPercentage > 90))
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, base.PrefixJobGuid($"Api rate limit is nearing threshold of 95 pct utilization|appPct={responseItem.AppUtilizationPercentage}|accountPct={responseItem.AccountUtilizationPercentage}|headers={String.Join(",", responseItem.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}" +
                        $"|Report={responseItem.ReportItem.ReportName}|status:{responseItem.ReportItem.EntityStatus}|url:{responseItem.ReportItem.RelativeUrl}|retryAfterInMinutes={responseItem.RetryAfterInMinutes}")));
                    logUtilizationWarning = false;
                }

                responseList.Add(responseItem);
            }

            //check all individual batch responses for any errors where we want to immediately abort and skip entity
            var suspendingErrors = responseList.Where(r => _facebookErrorList.Any(e => r.ErrorMessage.Contains(e.Message, StringComparison.InvariantCultureIgnoreCase) && !e.RetryPageSize));

            //check all individual batch responses for any errors where we want to retry, ie lower page size to reduce data size
            var retryErrors = responseList.Where(r => _facebookErrorList.Any(e => r.ErrorMessage.Contains(e.Message, StringComparison.InvariantCultureIgnoreCase) && e.RetryPageSize));

            if (suspendingErrors.Any())
            {
                var errorLog = suspendingErrors.Select(r => $"Report:{r.ReportItem.ReportName}-status:{r.ReportItem.EntityStatus}-url:{r.ReportItem.RelativeUrl}-code:{r.BatchItemResponse.Code}-message:{r.BatchItemResponse.Body}-headers:{String.Join(",", r.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}");
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"api response matches a known suspending error - stopping calls for this entity - details: {errorLog}")));
                foreach (var response in suspendingErrors)
                {
                    response.ReportItem.SkipEntity = true;
                }
            }
            else if (retryErrors.Any())
            {
                var errorLog = retryErrors.Select(r => $"Report:{r.ReportItem.ReportName}-status:{r.ReportItem.EntityStatus}-url:{r.ReportItem.RelativeUrl}-message:{r.BatchItemResponse.Body}-headers:{String.Join(",", r.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}");
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"api response matches error message prompting for a reduction in data page size - details: {errorLog}")));
                var retryReportList = reports.Where(x => retryErrors.Any(r => r.ReportItem.ReportName == x.ReportName));
                foreach (var report in retryReportList)
                {
                    report.IsDownloaded = false;
                    report.RetryPageSize = true;
                }
            }
            else
            {
                DownloadApiResponse(batchResponses, reports, queueItem, requestType, batchRequest, allReportItems);
                isSuccess = true;
            }
        }
        catch (HttpClientProviderRequestException e)
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(
                $"HttpClientProviderRequestException Error when submitting batch request -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  ->" +
                $"Exception details : {e}"), e));
            throw;
        }
        catch (Exception e)
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(
                $"Web Exception Error when submitting batch request -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  ->" +
                $"Error Message: -> Exception: {e.Message} -> STACK {e.StackTrace}"), e));
            throw;
        }

        return isSuccess;
    }

    private void DownloadApiResponse(List<BatchResponse> batchResponses, List<FacebookReportItem> reports, Queue queueItem, string requestType, ApiBatchRequest batchRequest
        , List<FacebookReportItem> allReportItems)
    {
        var totalComplete = 0;
        var exitLoop = false;
        for (var k = batchRequest.Index; k < batchResponses.Count; k++)
        {
            var reportItem = reports[k];
            var apiResponse = batchResponses[k];
            var result = apiResponse.Body;
            var headers = apiResponse.headers.ConvertAll(x => new { header = x.Name + ":" + x.Value });

            switch (apiResponse.Code)
            {
                case 200 when reportItem.IsReady:
                    continue;
                case 200:
                    switch (requestType)
                    {
                        case "DownloadInsights":
                            var insightsDownloaded = GetInsightsReportData(reportItem, apiResponse, queueItem, allReportItems);
                            if (!insightsDownloaded)
                            {
                                reportItem.TimeTracker.SaveSession(SessionTypeEnum.GH_DOWNLOAD_FILE, result.Length);
                                reportItem.IsReady = false;
                                reportItem.IsDownloaded = false;
                                reportItem.DownloadFailed = true;
                                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Download failed for report {reportItem.ReportName} - runid:{reportItem.ReportRunId} " +
                                    $"- fileguid:{reportItem.FileGuid} - url:{reportItem.RelativeInsightsUrl}")));
                                exitLoop = true;
                            }
                            break;
                        case "DownloadDimension":
                            var dimensionDownloaded = GetDimensionData(reportItem, apiResponse, queueItem, allReportItems);
                            if (!dimensionDownloaded)
                            {
                                reportItem.TimeTracker.SaveSession(SessionTypeEnum.GH_DOWNLOAD_FILE, result.Length);
                                // if download fails, then start over dimension call 
                                // and remove current file
                                reportItem.RelativeUrl = reportItem.OriginalUrl;
                                reportItem.IsReady = false;
                                reportItem.IsDownloaded = false;
                                var localFilePath = GetReportPath(reportItem, queueItem);
                                Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, localFilePath);
                                FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);
                                if (tempDestFile.Exists)
                                    tempDestFile.Delete();
                            }
                            break;
                        case "QueueReport":
                            GetReportRunId(result, reportItem, queueItem);
                            break;
                        case "CheckStatus":
                            var isValidStatus = GetInsightsReportStatus(result, reportItem);
                            if (!isValidStatus)
                            {
                                reportItem.TimeTracker.SaveSession(SessionTypeEnum.GRAPH_ASYNC_REPORT, result.Length);
                                reportItem.IsReady = true;
                                reportItem.StatusCheckFailed = true;
                                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, base.PrefixJobGuid($"Status check failed for report {reportItem.ReportName} - runid:{reportItem.ReportRunId} " +
                                    $"- fileguid:{reportItem.FileGuid} - url:{reportItem.RelativeUrl}")));
                            }
                            break;
                        default:
                            ResetBatchRequest(batchResponses, batchRequest, k, totalComplete);
                            throw new ArgumentException($"Internal request type {requestType} is unknown; unable to process 200 response.");
                    }
                    break;
                default:
                    //need to remove the recent responses to allow for Polly to retry and capture new results
                    ResetBatchRequest(batchResponses, batchRequest, k, totalComplete);
                    Task.Delay(300).Wait();
                    throw new APIResponseException($"error code: {apiResponse.Code} for call {reportItem.RelativeUrl} returned for Facebook {reportItem.ReportName} - message: {apiResponse.Body}; headers:{String.Join(",", headers)}");
            }
            if (exitLoop)
                break;

            totalComplete++;
        }
    }

    private static void ResetBatchRequest(List<BatchResponse> batchResponses, ApiBatchRequest batchRequest, int listIndex, int totalComplete)
    {
        var lastIndex = listIndex;
        var unprocessedResponses = batchResponses.Count - lastIndex;
        batchResponses.RemoveRange(lastIndex, unprocessedResponses);
        batchRequest.BatchOperations.RemoveRange(0, totalComplete);
        batchRequest.Index = lastIndex;
    }

    private static void GetReportRunId(string result, FacebookReportItem reportItem, Queue queueItem)
    {
        var adReport = ETLProvider.DeserializeType<InsightStatsReport>(result);
        var runReportId = adReport.ReportRunId;
        reportItem.ReportRunId = runReportId;

        reportItem.TimeTracker.StartSession(SessionTypeEnum.GRAPH_ASYNC_REPORT);
    }

    private bool GetInsightsReportStatus(string result, FacebookReportItem reportItem)
    {
        var returnVal = false;
        try
        {
            var reportStatus = ETLProvider.DeserializeType<ReportStatus>(result);

            if (
                (string.IsNullOrEmpty(reportStatus.AsyncStatus) &&
                 (reportStatus.AsyncPercentCompletion == "0")) ||
                reportStatus.AsyncStatus.Equals("Job Running",
                    StringComparison.InvariantCultureIgnoreCase) ||
                reportStatus.AsyncStatus.Equals("Job Started",
                    StringComparison.InvariantCultureIgnoreCase) ||
                reportStatus.AsyncStatus.Equals("Job Not Started",
                    StringComparison.InvariantCultureIgnoreCase)
            )
            {
                returnVal = true;
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    base.PrefixJobGuid(
                        $"Status for {reportItem.ReportName} report run id {reportStatus.ReportRunId} is {reportStatus.AsyncStatus} and completion percentage is {reportStatus.AsyncPercentCompletion}")));
            }
            else if (reportStatus.AsyncStatus.Equals("Job Completed",
                StringComparison.InvariantCultureIgnoreCase))
            {
                returnVal = true;
                reportItem.IsReady = true;
                reportItem.OriginalInsightsUrl =
                    $"{_credential.CredentialSet.Version}/{reportItem.ReportRunId}/insights/?limit=1000";
                reportItem.RelativeInsightsUrl =
                    $"{_credential.CredentialSet.Version}/{reportItem.ReportRunId}/insights/?limit=1000";

                reportItem.TimeTracker.SaveSession(SessionTypeEnum.GRAPH_ASYNC_REPORT, result.Length);
            }
            else
            {
                returnVal = false;
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, base.PrefixJobGuid(
                    $"Error GetInsightsReportStatus -> failed on: {reportItem.FileGuid} for EntityID: {reportItem.AccountID} -> " +
                    $"Bad report status for {reportItem.ReportName}; report run id {reportStatus.ReportRunId} is {reportStatus.AsyncStatus} and completion percentage is {reportStatus.AsyncPercentCompletion}")));
            }
        }
        catch (Exception exc)
        {
            returnVal = false;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                    base.PrefixJobGuid($"Error GetInsightsReportStatus - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.AccountID} " +
                    $" Report Name: {reportItem.ReportName}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
        }

        return returnVal;
    }

    private bool GetDimensionData(FacebookReportItem reportItem, BatchResponse apiResponse, Queue queueItem, List<FacebookReportItem> dimReportList)
    {
        bool returnVal = false;
        try
        {
            reportItem.IsReady = true;
            var paths = GetReportPath(reportItem, queueItem);

            Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, paths);
            var tempDestFile = new FileSystemFile(tempDestUri);

            if (!tempDestFile.Directory.Exists)
            {
                tempDestFile.Directory.Create();
            }

            reportItem.TimeTracker.StartSession(SessionTypeEnum.GH_DOWNLOAD_FILE);

            WriteDimensionDataToLocalFile(reportItem, apiResponse, tempDestUri, dimReportList);

            reportItem.TimeTracker.SaveSession(SessionTypeEnum.GH_DOWNLOAD_FILE, apiResponse.Body.Length);

            returnVal = true;

            var uploadReady = dimReportList.Where(r => r.ReportName.Equals(reportItem.ReportName, StringComparison.InvariantCultureIgnoreCase)).All(x => x.IsDownloaded);
            if (!uploadReady)
                return returnVal;

            var totalApiCalls = dimReportList.Where(r => r.ReportName.Equals(reportItem.ReportName, StringComparison.InvariantCultureIgnoreCase)).Sum(x => x.RetryAttempt);
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Report: {reportItem.ReportName} - writing COMPLETE. api calls made: {totalApiCalls} " +
                $"Account ID: {reportItem.AccountID}; Report: {reportItem.ReportName}; Relative url: {reportItem.RelativeUrl}")));

            reportItem.TimeTracker.StartSession(SessionTypeEnum.GH_STAGE_FILE);

            string[] stagePaths =
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), reportItem.FileCollectionItem.FilePath
            };

            var rawFile = new S3File(RemoteUri.CombineUri(_baseRawDestUri, stagePaths), GreenhouseS3Creds);
            base.UploadToS3(tempDestFile, rawFile, stagePaths);

            var files = queueItem.FileCollection?.ToList();
            if (files == null)
                files = new List<FileCollectionItem>();

            FileCollectionItem fileItem = new FileCollectionItem()
            {
                FileSize = rawFile.Length,
                SourceFileName = reportItem.FileCollectionItem.SourceFileName,
                FilePath = reportItem.FileCollectionItem.FilePath
            };
            files.Add(fileItem);
            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
            queueItem.FileSize += rawFile.Length;

            reportItem.TimeTracker.SaveSession(SessionTypeEnum.GH_STAGE_FILE, rawFile.Length);

            //updating report-state date here, but report-state is not updated in database until after files are staged
            _currentReportState.DeltaDate = _importDateTime.Date;
        }
        catch (WebException wex)
        {
            returnVal = false;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                 base.PrefixJobGuid($"Web Exception Error downloading dimension report - failed on queueID: {reportItem.QueueID} " +
                    $"for EntityID: {reportItem.AccountID} Report Name: {reportItem.ReportName} ->" +
                    $"Error -> Exception: {wex.Message}"), wex));
        }
        catch (Exception exc)
        {
            returnVal = false;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                    base.PrefixJobGuid($"Error downloading simension report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.AccountID} " +
                    $" Report Name: {reportItem.ReportName}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
        }

        return returnVal;
    }

    private static string[] GetReportPath(FacebookReportItem reportItem, Queue queueItem)
    {
        string[] paths =
        {
            queueItem.FileGUID.ToString(), GetDatedPartition(queueItem.FileDate), reportItem.FileCollectionItem.FilePath
        };

        return paths;
    }

    private static void WriteDimensionDataToLocalFile(FacebookReportItem reportItem, BatchResponse apiResponse, Uri tempDestUri, List<FacebookReportItem> dimReportList)
    {
        if (!File.Exists(tempDestUri.LocalPath))
            File.Create(tempDestUri.LocalPath).Dispose();
        using (StreamWriter rawWriter = File.AppendText(tempDestUri.LocalPath))
        {
            bool next = false;
            var result = apiResponse.Body;

            //check and assign new relative url with next page of data
            switch (reportItem.ReportName)
            {
                case "Ad":
                    AdDimension ad = ETLProvider.DeserializeType<AdDimension>(result);
                    next = (ad?.paging?.next != null &&
                            ad?.paging?.cursors?.after != null);
                    if (next)
                    {
                        reportItem.RelativeUrl =
                            $"{reportItem.OriginalUrl}&after={ad.paging.cursors.after}";
                    }

                    break;
                case "Campaign":
                    AdCampaignDimension acd =
                        ETLProvider.DeserializeType<AdCampaignDimension>(result);
                    next = (acd?.paging?.next != null &&
                            acd?.paging?.cursors?.after != null);
                    if (next)
                    {
                        reportItem.RelativeUrl =
                            $"{reportItem.OriginalUrl}&after={acd.paging.cursors.after}";
                    }

                    break;
                case "AdSet":
                    AdSetDimension asd =
                        ETLProvider.DeserializeType<AdSetDimension>(result);
                    next = (asd?.paging?.next != null &&
                            asd?.paging?.cursors?.after != null);
                    if (next)
                    {
                        reportItem.RelativeUrl =
                            $"{reportItem.OriginalUrl}&after={asd.paging.cursors.after}";
                    }

                    break;
                case "AdCreative":
                    AdCreatives ac =
                        ETLProvider.DeserializeType<AdCreatives>(result);

                    result = $"{{'ad_id':'{reportItem.EntityID}','raw_data':{result}}}";

                    next = (ac?.paging?.next != null &&
                            ac?.paging?.cursors?.after != null);
                    if (next)
                    {
                        reportItem.RelativeUrl =
                            $"{reportItem.OriginalUrl}&after={ac.paging.cursors.after}";
                    }

                    break;
            }

            rawWriter.Write(result);

            if (next)
            {
                rawWriter.Write(",");
            }
            else
            {
                reportItem.IsDownloaded = true;
            }

            var allPartsComplete = dimReportList.Where(x => x.ReportName.Equals(reportItem.ReportName, StringComparison.InvariantCultureIgnoreCase)).All(y => y.IsReady && y.IsDownloaded);
            if (!next && !allPartsComplete)
            {
                rawWriter.Write(",");
            }
        }
    }

    private bool QueueReport(Queue queueItem, List<FacebookReportItem> insightsReportList, QueueInfo queueInfo, ReportManager reportManager, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        bool returnValue = false;
        try
        {
            // if there is a current snapshot then we can avoid retrieving the ad dimension list
            var currentSnapshot = reportManager.Snapshots.Find(r => r.QueueID == queueItem.ID);
            var adDimensionList = currentSnapshot == null ? GetAdIds(queueItem, queueItem.FileGUID.ToString(), queueInfo, reportList) : new List<DataAdDimension>();

            var isDailyJob = !queueItem.IsBackfill;
            var skipEntity = false;
            var insightsReports = reportList.Where(r => r.ReportSettings.ReportType.Equals("insights", StringComparison.InvariantCultureIgnoreCase) && r.IsActive && r.ReportSettings.IsSummaryReport == false).ToList();

            var reportRequest = new GraphApiRequest(_httpClientProvider, graphEndpoint, JobGUID, _credential.CredentialSet.AccessToken, _credential.CredentialSet.Version);
            var usePreset = CheckFileDate(queueItem, queueInfo);

            foreach (var factReport in insightsReports)
            {
                var reportItems = reportManager.GetInsightsReportItems(queueItem, factReport, adDimensionList, reportRequest, usePreset);
                insightsReportList.AddRange(reportItems);
            }

            var reportsToRun = insightsReportList.Where(x => x.ReportRunId == null).ToList();

            if (reportsToRun.Count == 0)
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"No report items to be queued up for account id: {queueItem.EntityID}; File Date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}")));
                return true;
            }

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"START - Get report IDs; AccountID: {queueItem.EntityID}; File Date: {queueItem.FileDate}; total report items: {insightsReportList.Count}")));

            bool allDone = false;

            ResetRetryAttemptForAllReports(insightsReportList);

            do
            {
                var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
                {
                    Counter = 0,
                    MaxRetry = backoffDetailsSummary.MaxRetry
                };

                reportsToRun = insightsReportList.Where(x => x.ReportRunId == null).ToList();

                var httpMethod = System.Net.Http.HttpMethod.Post.ToString();

                MakeBatchRequest(reportsToRun, queueItem, apiCallsBackOffStrategy, httpMethod, false, "QueueReport");

                skipEntity = reportsToRun.Any(x => x.SkipEntity == true);
                allDone = reportsToRun.All(x => x.ReportRunId != null) || skipEntity;

                if (allDone) continue;

                if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
                {
                    //the runtime is greater than the max RunTime
                    throw new AllotedRunTimeExceededException($"The runtime ({runtime.Elapsed}) exceeded the allotted time {maxRuntime}");
                }

                var reportExceedingMaxAttempt =
                    reportsToRun.Find(x => x.RetryAttempt > _maxDownloadAttempt);

                if (reportExceedingMaxAttempt != null)
                {
                    throw new APIReportException(PrefixJobGuid(
                        $"The Report Id={reportExceedingMaxAttempt.ReportRunId} is not available after the {_maxDownloadAttempt}th attempt. Stopping the import. " +
                        $"Account ID: {reportExceedingMaxAttempt.AccountID}; Report: {reportExceedingMaxAttempt.ReportName}; Relative url: {reportExceedingMaxAttempt.RelativeUrl}"));
                }

                var maxNumberOfRetryReport = reportsToRun.Max(x => x.RetryAttempt);
                int delay = (int)Math.Pow(1.4, maxNumberOfRetryReport) * 1000;
                Task.Delay(delay).Wait();
            } while (!allDone);

            if (skipEntity) return returnValue;
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"END - Get report IDs; All reports are queued up and have a report ID for account id: {queueItem.EntityID}; File Date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}")));
            returnValue = true;
        }
        catch (Exception exc)
        {
            exceptionCounter++;
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Error, deleteQueueItem: false);
            _queueItems.First(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Error;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                base.PrefixJobGuid(
                    $"Error queueing insights reports -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}")
                , exc));
            returnValue = false;
        }
        return returnValue;
    }

    private bool CheckStatusAndDownloadReport(Queue queueItem, List<FacebookReportItem> insightsReportList, QueueInfo queueInfo, ReportManager reportManager, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        bool returnValue = false;
        try
        {
            CleanupLocalEntityFolder(queueItem.FileGUID.ToString());

            var allReportItemsHaveReportRunId = insightsReportList.All(x => x.ReportRunId != null);

            if (allReportItemsHaveReportRunId)
            {
                var readyToDownload = insightsReportList.All(x => x.IsReady == true);

                if (!readyToDownload)
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"START - Check report ID status; AccountID: {queueItem.EntityID}; File Date: {queueItem.FileDate}; total report items: {insightsReportList.Count}")));

                    CheckStatus(queueItem, insightsReportList);

                    // Snapshot # 2
                    reportManager.TakeSnapshot(insightsReportList, queueItem);
                }

                var failedCheck = insightsReportList.Any(x => x.StatusCheckFailed);
                readyToDownload = insightsReportList.All(x => x.IsReady == true);

                if (!readyToDownload || failedCheck)
                {
                    throw new ReportNotReadyException(PrefixJobGuid($"All reports NOT ready for download or status check invalid for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}."));
                }

                DownloadInsightsReport(insightsReportList, queueItem, reportList);

                var allDone = insightsReportList.All(x => x.IsReady && x.IsDownloaded);

                if (!allDone)
                {
                    throw new APIReportException(PrefixJobGuid($"Failed to download all report items for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}."));
                }

                // download dimension data once insights async reports are all complete
                var dimReportList = new List<FacebookReportItem>();

                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{queueItem.FileGUID.ToString()}-Start DownloadDimensionData({queueItem.EntityID})-date={queueItem.FileDate}")));

                var dimFilesReady = DownloadDimensionData(queueItem, dimReportList, queueInfo, reportManager, reportList);

                // log time spent getting dimension reports
                LogRecordedTime(queueItem, dimReportList);

                //if dimension files are not available then this queue should fail and all others for same entity
                if (!dimFilesReady)
                {
                    throw new APIReportException(PrefixJobGuid($"Failed to download dimension reports for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}."));
                }

                S3UploadCompleteInsightsReports(queueItem, insightsReportList);

                returnValue = true;
            }
            else
            {
                throw new APIReportException($"Not all report items have a job run ID for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}.");
            }
        }
        //catch report not ready exc here, 
        catch (ReportNotReadyException exc)
        {
            warningCounter++;
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Pending, deleteQueueItem: false);
            _queueItems.FirstOrDefault(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Pending;
            _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, base.PrefixJobGuid(exc.ToString())));
            returnValue = false;
        }
        catch (Exception exc)
        {
            exceptionCounter++;
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Error, deleteQueueItem: false);
            _queueItems.FirstOrDefault(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Error;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                base.PrefixJobGuid(
                    $"Error downloading Insights report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}")
                , exc));
            returnValue = false;
        }
        return returnValue;
    }

    private void CheckStatus(Queue queueItem, List<FacebookReportItem> insightsReportList)
    {
        var allDone = false;
        ResetRetryAttemptForAllReports(insightsReportList);

        do
        {
            var apiCallsBackOffStrategy = new BackOffStrategy
            {
                Counter = 0,
                MaxRetry = 10
            };

            var reports = insightsReportList.Where(x => !x.IsReady).ToList();

            if (reports.Count != 0)
            {
                var httpMethod = System.Net.Http.HttpMethod.Get.ToString();

                MakeBatchRequest(reports, queueItem, apiCallsBackOffStrategy, httpMethod, true, "CheckStatus");
            }

            if (reports.Any(x => x.SkipEntity == true)) break;

            allDone = reports.All(x => x.IsReady == true);
            if (allDone) continue;

            if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
            {
                //the runtime is greater than the max RunTime
                throw new AllotedRunTimeExceededException($"The runtime ({runtime.Elapsed}) exceeded the allotted time {maxRuntime}");
            }

            var reportExceedingMaxAttempt = reports.Find(x => x.RetryAttempt > _maxDownloadAttempt);
            if (reportExceedingMaxAttempt != null)
            {
                throw new APIReportException(PrefixJobGuid(
                    $"The Report Id={reportExceedingMaxAttempt.ReportRunId} is not available after the {_maxDownloadAttempt}th attempt. Stopping the import." +
                    $"Account ID: {reportExceedingMaxAttempt.AccountID}; Report: {reportExceedingMaxAttempt.ReportName}; Relative url: {reportExceedingMaxAttempt.RelativeUrl}"));
            }

            var maxNumberOfRetryReport = reports.Max(x => x.RetryAttempt);
            int delay = (int)Math.Pow(1.4, maxNumberOfRetryReport) * 1000;
            Task.Delay(delay).Wait();
        } while (!allDone);

        if (insightsReportList.Any(x => x.SkipEntity == true) || insightsReportList.Any(x => x.StatusCheckFailed == true)) return;
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            base.PrefixJobGuid(
                $"All reports are ready to be downloaded for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}.")));
    }

    private void DownloadInsightsReport(List<FacebookReportItem> insightsReportList, Queue queueItem, List<MappedReportsResponse<FacebookReportSettings>> reportList)
    {
        bool allDone = false;
        var retryCounter = 0;

        do
        {
            //creating new BackOffStrategy to use specifically for downloading reports
            var apiCallsForDownloadBackOffStrategy = new BackOffStrategy
            {
                Counter = 0,
                MaxRetry = backoffDetailsSummary.MaxRetry
            };

            var isSuccess = CheckAPIPagingLimit(queueItem, insightsReportList, reportList, ref retryCounter, true);

            if (isSuccess)
            {
                var httpMethod = System.Net.Http.HttpMethod.Get.ToString();
                var reports = insightsReportList.Where(x => !x.IsDownloaded).ToList();
                MakeBatchRequest(reports, queueItem, apiCallsForDownloadBackOffStrategy, httpMethod, true, "DownloadInsights", insightsReportList);
            }

            if (insightsReportList.Any(x => x.SkipEntity == true) || insightsReportList.Any(x => x.DownloadFailed == true)) break;
            allDone = insightsReportList.All(x => x.IsDownloaded);
        } while (!allDone);

        if (insightsReportList.Any(x => x.SkipEntity == true) || insightsReportList.Any(x => x.DownloadFailed == true)) return;
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            base.PrefixJobGuid(
                $"All reports are downloaded for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}.")));
    }

    private bool GetInsightsReportData(FacebookReportItem reportItem, BatchResponse apiResponse, Queue queueItem, List<FacebookReportItem> insightsReportList)
    {
        var returnVal = false;
        try
        {
            reportItem.TimeTracker.StartSession(SessionTypeEnum.GH_DOWNLOAD_FILE);
            var paths = GetReportPath(reportItem, queueItem);

            Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, paths);
            var tempDestFile = new FileSystemFile(tempDestUri);

            if (!tempDestFile.Directory.Exists)
            {
                tempDestFile.Directory.Create();
            }

            WriteInsightsDataToLocalFile(reportItem, apiResponse, tempDestUri, insightsReportList);

            reportItem.TimeTracker.SaveSession(SessionTypeEnum.GH_DOWNLOAD_FILE, apiResponse.Body.Length);
            returnVal = true;
        }
        catch (Exception exc)
        {
            returnVal = false;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                    base.PrefixJobGuid($"Error downloading insights report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.AccountID} " +
                    $" Report Name: {reportItem.ReportName} Report ID: {reportItem.ReportRunId} url: {reportItem.RelativeInsightsUrl}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
        }

        return returnVal;
    }

    private static void WriteInsightsDataToLocalFile(FacebookReportItem reportItem, BatchResponse apiResponse, Uri tempDestUri, List<FacebookReportItem> insightsReportList)
    {
        using (StreamWriter rawWriter = File.AppendText(tempDestUri.LocalPath))
        {
            bool next = false;

            var result = apiResponse.Body;
            InsightStatsReport reportData =
                ETLProvider.DeserializeType<InsightStatsReport>(result);
            next = (reportData?.paging?.next != null &&
                    reportData?.paging?.cursors?.after != null);

            rawWriter.Write(result);

            reportItem.IsReady = true;

            if (next)
            {
                reportItem.RelativeInsightsUrl =
                    $"{reportItem.OriginalInsightsUrl}&after={reportData.paging.cursors.after}";

                rawWriter.Write(",");
            }
            else
            {
                reportItem.IsDownloaded = true;
            }

            var allPartsComplete = insightsReportList
                .Where(x => x.ReportName.Equals(reportItem.ReportName, StringComparison.InvariantCultureIgnoreCase))
                .All(y => y.IsReady && y.IsDownloaded);
            if (!next && !allPartsComplete)
            {
                rawWriter.Write(",");
            }
        }
    }

    private static bool CheckFileDate(Queue queueItem, QueueInfo queueInfo)
    {
        // if daily queue record is considered delayed
        // then we cannot use the preset value for FB
        // and instead use the file log date for start and end time
        var usePreset = false;
        if (!queueItem.IsBackfill)
        {
            // Date Tracker tracks queue records within lookback range of the daily job
            // if queue item is not found in the Date Tracker then it is outside of the lookback range
            // and should not use the date-present that is based on the lookback range
            if (queueInfo != null)
                usePreset = true;
        }

        return usePreset;
    }

    private void S3UploadCompleteInsightsReports(Queue queueItem, List<FacebookReportItem> insightsReportList)
    {
        var emptyReports = insightsReportList.Where(x => x.ReportRunId == "0");
        foreach (var report in emptyReports)
        {
            report.IsDownloaded = CreateEmptyReport(queueItem, report);
        }

        var incompleteReports = insightsReportList.Where(x => !x.IsDownloaded);
        var completeReports = insightsReportList
            .Except(insightsReportList.Where(
                r => incompleteReports.Select(i => i.ReportName).Contains(r.ReportName)))
                .GroupBy(x => x.ReportName).Select(grp => grp.First());

        foreach (var report in completeReports)
        {
            //skip upload where report ID is zero, aka empty file report
            if (report.ReportRunId == "0") continue;

            report.TimeTracker.StartSession(SessionTypeEnum.GH_STAGE_FILE);

            var paths = GetReportPath(report, queueItem);

            Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, paths);
            IFile tempDestFile = new FileSystemFile(tempDestUri);

            string[] stagePaths =
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.FileCollectionItem.FilePath
            };

            var rawFile = new S3File(RemoteUri.CombineUri(_baseRawDestUri, stagePaths), GreenhouseS3Creds);
            base.UploadToS3(tempDestFile, rawFile, stagePaths);

            var files = queueItem.FileCollection?.ToList();
            if (files == null)
                files = new List<FileCollectionItem>();

            FileCollectionItem fileItem = new FileCollectionItem()
            {
                FileSize = rawFile.Length,
                SourceFileName = report.FileCollectionItem.SourceFileName,
                FilePath = report.FileCollectionItem.FilePath
            };
            files.Add(fileItem);
            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
            queueItem.FileSize += rawFile.Length;

            report.TimeTracker.SaveSession(SessionTypeEnum.GH_STAGE_FILE, rawFile.Length);
        }

        CleanupLocalEntityFolder(queueItem.FileGUID.ToString());
    }

    private void CleanupLocalEntityFolder(string fileGUID)
    {
        Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, fileGUID);
        FileSystemDirectory localImportDirectory = new FileSystemDirectory(tempLocalImportUri);
        if (localImportDirectory.Exists)
        {
            localImportDirectory.Delete(true);
        }
    }

    private static void ResetRetryAttemptForAllReports(List<FacebookReportItem> insightsReportList)
    {
        foreach (var report in insightsReportList)
        {
            report.RetryAttempt = 0;
        }
    }

    private void WriteObjectToFile(IEnumerable<object> entity, Queue queue, FileCollectionItem fileCollectionItem)
    {
        string[] paths = new string[]
        {
            queue.EntityID.ToLower(), GetDatedPartition(queue.FileDate), fileCollectionItem.FilePath
        };

        S3File transformedFile = new S3File(RemoteUri.CombineUri(_baseStageDestUri, paths), GreenhouseS3Creds);

        if (transformedFile.Exists)
            transformedFile.Delete();

        if (!entity.Any())
        {
            // create empty stage file
            Stream rawFileStream = transformedFile.Create();
            rawFileStream.Close();
            fileCollectionItem.FileSize = 0;
            return;
        }

        string[] csvPaths = { queue.FileGUID.ToString(), GetDatedPartition(queue.FileDate), fileCollectionItem.FilePath };

        Uri tempDestUri = RemoteUri.CombineUri(_baseLocalImportUri, csvPaths);
        var tempDestFile = new FileSystemFile(tempDestUri);

        if (!tempDestFile.Directory.Exists)
        {
            tempDestFile.Directory.Create();
        }

        UtilsIO.WriteToCSV(entity, tempDestFile.FullName);

        base.UploadToS3(tempDestFile, transformedFile, paths);

        fileCollectionItem.FileSize = transformedFile.Length;
    }

    private void StageFacebookFiles(Queue queueItem, QueueInfo queueDateTracker, QueueDateTracker dateTracker, ReportManager reportManager)
    {
        try
        {
            if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"File Collection is empty; unable to stage data for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} ")));
            }
            else
            {
                var stageFileTimer = new Stopwatch();
                stageFileTimer.Start();
                var reports = queueItem.FileCollection;
                Action<IEnumerable<object>, Queue, FileCollectionItem> writeToFileSignature = ((a, b, c) => WriteObjectToFile(a, b, c));

                // create new file collection based on stage files
                // and save this file collection as the queue's file collection 
                // to be used for data load
                List<FileCollectionItem> stageFileCollection = new List<FileCollectionItem>();
                foreach (var report in reports)
                {
                    string[] paths = new string[]
                    {
                        queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.FilePath
                    };

                    IFile rawFile = _rac.WithFile(RemoteUri.CombineUri(_baseRawDestUri, paths));
                    string rawText;
                    using (var sr = new System.IO.StreamReader(rawFile.Get()))
                    {
                        rawText = sr.ReadToEnd();
                    }

                    bool hasData;
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Staging data for report: {report.SourceFileName} for FileGUID: {queueItem.FileGUID}")));

                    switch (report.SourceFileName)
                    {
                        case "Ad":
                            var allAdDimension = ETLProvider.DeserializeType<AllData<DataAdDimension>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdDimension.allData.Count != 0 && allAdDimension.allData != null;
                            var adDimensionList = hasData ? allAdDimension.allData.ToList() : new List<DataAdDimension>();
                            FacebookService.LoadFacebookAdDimension(adDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdSet":
                            var allAdSet = ETLProvider.DeserializeType<AllData<AdSetDimension>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdSet.allData.Count != 0 && allAdSet.allData.First().data != null;
                            var adSetDimensionList = hasData ? allAdSet.allData.SelectMany(a => a.data).ToList() : new List<DataAdSetDimension>();
                            FacebookService.LoadFacebookAdSetDimension(adSetDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdAccount":
                            var allAdAccount = ETLProvider.DeserializeType<AllData<AdAccountDimension>>($"{{'allData':[{rawText}]}}");
                            List<AdAccountDimension> adAccountDimensionList = allAdAccount.allData;
                            FacebookService.LoadFacebookAdAccountDimension(adAccountDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdStatsReport":
                            var allAdStatsReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdStatsReport.allData.Count != 0 && allAdStatsReport.allData.First().data != null;
                            var adStatsReportList = hasData ? allAdStatsReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdStatReport(adStatsReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdCreative":
                            var allAdCreativeDimension = ETLProvider.DeserializeType<AllData<AdCreativeDimensionWithRequestAdId>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdCreativeDimension.allData.Count != 0 && allAdCreativeDimension.allData.First().CreativeData != null;
                            var adCreativeDimensionList = hasData ? allAdCreativeDimension.allData.ToList() : new List<AdCreativeDimensionWithRequestAdId>();
                            FacebookService.LoadFacebookAdCreativeDimension(adCreativeDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "Campaign":
                            var allAdCampaignDimension = ETLProvider.DeserializeType<AllData<AdCampaignDimension>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdCampaignDimension.allData.Count != 0 && allAdCampaignDimension.allData.First().data != null;
                            var adCampaignDimensionList = hasData ? allAdCampaignDimension.allData.SelectMany(a => a.data).ToList() : new List<DataAdCampaignDimension>();
                            FacebookService.LoadFacebookAdCampaignDimension(adCampaignDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdCampaignStatsReport":
                            var allAdCampaignStatsReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdCampaignStatsReport.allData.Count != 0 && allAdCampaignStatsReport.allData.First().data != null;
                            var adCampaignStatsReportList = hasData ? allAdCampaignStatsReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdCampaignStatsReport(adCampaignStatsReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdSetStatsReport":
                            var allAdSetStatsReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdSetStatsReport.allData.Count != 0 && allAdSetStatsReport.allData.First().data != null;
                            var adSetStatsReportList = hasData ? allAdSetStatsReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdSetStatsReport(adSetStatsReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdStatsByDMAReport":
                            var allAdStatsByDmaReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdStatsByDmaReport.allData.Count != 0 && allAdStatsByDmaReport.allData.First().data != null;
                            var adStatsByDmaReportList = hasData ? allAdStatsByDmaReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdStatByDmaReport(adStatsByDmaReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdStatsByCountryReport":
                            var allAdStatsByCountryReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdStatsByCountryReport.allData.Count != 0 && allAdStatsByCountryReport.allData.First().data != null;
                            var adStatsByCountryReportList = hasData ? allAdStatsByCountryReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdStatByCountryReport(adStatsByCountryReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "CustomConversions":
                            var allCustomConversionsDimension = ETLProvider.DeserializeType<AllData<CustomConversion>>($"{{'allData':[{rawText}]}}");
                            hasData = allCustomConversionsDimension.allData.Count != 0 && allCustomConversionsDimension.allData.First().data != null;
                            var customConversionsDimensionList = hasData ? allCustomConversionsDimension.allData.SelectMany(a => a.data).ToList() : new List<DataCustomConversionDimension>();
                            FacebookService.LoadCustomConversionsDimension(customConversionsDimensionList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "Campaign-Reach":
                            var allAdCampaignReachReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdCampaignReachReport.allData.Count != 0 && allAdCampaignReachReport.allData.First().data != null;
                            var adCampaignReachReportList = hasData ? allAdCampaignReachReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdCampaignReachReport(adCampaignReachReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "Adset-Reach":
                            var allAdSetReachReport = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdSetReachReport.allData.Count != 0 && allAdSetReachReport.allData.First().data != null;
                            var adSetReachReportList = hasData ? allAdSetReachReport.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdSetReachReport(adSetReachReportList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        case "AdStats-Action-Reaction":
                            var allAdStatsActionReactions = ETLProvider.DeserializeType<AllData<InsightStatsReport>>($"{{'allData':[{rawText}]}}");
                            hasData = allAdStatsActionReactions.allData.Count != 0 && allAdStatsActionReactions.allData.First().data != null;
                            var adStatsActionReactionsList = hasData ? allAdStatsActionReactions.allData.SelectMany(a => a.data).ToList() : new List<StatsReportData>();
                            FacebookService.LoadFacebookAdStatActionReactions(adStatsActionReactionsList, queueItem, stageFileCollection, writeToFileSignature);
                            break;
                        default:
                            throw new NotSupportedException($"The Facebook report {report.SourceFileName} is not supported and has no matching POCO");
                    }
                }

                stageFileTimer.Stop();
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid(
                    $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID};StagingMin:{stageFileTimer.Elapsed.TotalMinutes}")));

                queueItem.Status = Constants.JobStatus.Complete.ToString();
                queueItem.StatusId = (int)Constants.JobStatus.Complete;
                queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(stageFileCollection);
                queueItem.FileSize += stageFileCollection.Sum(x => x.FileSize);
                JobService.Update((Queue)queueItem);

                // Update any pending queues for the daily job that are within the lookback range (ie included in Date Tracker)
                if (queueDateTracker != null)
                {
                    if (queueDateTracker.IsPrimaryDate)
                        _currentReportState.DailyCompletionDate = queueItem.FileDate;

                    var pendingDates = dateTracker.QueueInfoList.Where(x => x.EntityID == queueDateTracker.EntityID && !x.IsPrimaryDate);
                    var queuesToDelete = _queueItems.Where(x => pendingDates.Any(d => d.QueueID == x.ID));
                    if (queuesToDelete.Any())
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"The following queues were marked as import-complete and deleted as 1 queue will contain all reports." +
                            $" Deleting queue IDs={string.Join(", ", queuesToDelete.Select(q => q.ID))}")));
                        UpdateQueueWithDelete(queuesToDelete, Constants.JobStatus.Complete, true);
                    }
                }

                // add new dimension IDs to download complete in the s3 file
                reportManager.SaveDownloadedDimensions(queueItem.EntityID);

                // clear snapshot
                var currentSnapshot = reportManager.Snapshots.Find(x => x.QueueID == queueItem.ID);
                reportManager.Snapshots.Remove(currentSnapshot);

                SaveState();
            }
        }
        catch (Exception exc)
        {
            exceptionCounter++;
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Error, deleteQueueItem: false);
            _queueItems.First(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Error;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(
                $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.GetType().FullName} - Message: {exc.Message} - STACK {exc.StackTrace}")
                , exc));
        }
    }

    private void SaveState()
    {
        var dbState = SetupService.GetById<Lookup>(_facebookReportStateName);

        if (dbState != null)
        {
            var facebookStateLookup = new Lookup
            {
                Name = _facebookReportStateName,
                Value = JsonConvert.SerializeObject(_facebookReportState)
            };
            SetupService.Update(facebookStateLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(_facebookReportStateName, JsonConvert.SerializeObject(_facebookReportState));
        }
    }

    private bool CreateEmptyReport(Queue queueItem, FacebookReportItem report)
    {
        bool returnValue = true;
        try
        {
            string[] stagePaths =
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.FileCollectionItem.FilePath
            };

            S3File rawFile = new S3File(RemoteUri.CombineUri(base.GetDestinationFolder(), stagePaths), GreenhouseS3Creds);
            if (rawFile.Exists)
                rawFile.Delete();

            Stream rawFileStream = rawFile.Create();
            rawFileStream.Close();

            var files = queueItem.FileCollection?.ToList();
            if (files == null)
                files = new List<FileCollectionItem>();

            FileCollectionItem fileItem = new FileCollectionItem()
            {
                FileSize = rawFile.Length,
                SourceFileName = report.FileCollectionItem.SourceFileName,
                FilePath = report.FileCollectionItem.FilePath,
            };

            files.Add(fileItem);
            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
            queueItem.FileSize += rawFile.Length;
        }
        catch (Exception exc)
        {
            exceptionCounter++;
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                    base.PrefixJobGuid($"Error creating empty report -> failed on queue ID: {queueItem.ID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate.ToString("yyyy-MM-dd")} -> Exception: {exc.Message} - STACK {exc.StackTrace}")
                , exc));

            returnValue = false;
        }
        return returnValue;
    }

    private void SkipEntity(Queue queueItem, string previousMethod, ReportManager reportManager)
    {
        var skipEntityException = new APIReportException($"FAILED - skipping entity; failed at step {previousMethod}");
        exceptionCounter++;
        var unfinishedQueues = _queueItems.Where(q => q.EntityID.Equals(queueItem.EntityID, StringComparison.InvariantCultureIgnoreCase) && q.Status != Constants.JobStatus.Complete.ToString()).Select(x => x.ID).Distinct().Select(x => new Queue { ID = x });
        UpdateQueueWithDelete(unfinishedQueues, Common.Constants.JobStatus.Error, false);
        _queueItems.First(x => x.ID == queueItem.ID).StatusId = (int)Constants.JobStatus.Error;

        reportManager.ClearSnapshot(queueItem);

        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Skipping all queue reports at entity level -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {skipEntityException.Message}"), skipEntityException));
    }
}

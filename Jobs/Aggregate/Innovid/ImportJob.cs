using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Innovid;
using Greenhouse.Data.DataSource.Innovid;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Innovid;
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
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

// Initialize Aggregate generates 1 queue per day and clientID
// To minimize the number of calls to Innovid API, Import job bundles clients per day per is-backfill flag.
// 1 report downloaded will contain information for multiple clientIDs
// This means that multiple queues will reference the same downloaded report.
// At the end of import, we will only mark 1 queue as Processing pending.
// All the other queues will be marked as Processing Complete and removed from the queue.					

namespace Greenhouse.Jobs.Aggregate.Innovid;

[Export("Innovid-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient _remoteAccessClient;
    private ApiClient _apiClient;

    private Uri _baseDestUri;
    private int _threadSleepDelay;
    private int _maxPendingReports;
    private string _jobGuid => base.JED.JobGUID.ToString();
    private IOrderedEnumerable<OrderedQueue> _queueItems;
    private IEnumerable<Data.Model.Aggregate.APIReport<InnovidReportSettings>> _reports;
    private List<WeightConfiguration> _weightConfigurations;
    private int _defaultWeight;
    private int _dailyLookback;
    private readonly Stopwatch _runtime = new Stopwatch();

    private TimeSpan _maxRuntime;
    private int _exceptionCount;
    private int _warningCounter;
    private int _break_constant;
    private int _break_per_report;
    private int _check_report_status;
    private int _maxRetry;
    private List<ApiBundle> _unfinishedBundles;
    private List<ApiBundle> _bundlesNotImportedThisTime;
    private string _unfinishedReportsKey => $"{Constants.INNOVID_UNFINISHED_REPORTS}_{CurrentIntegration.IntegrationID}";
    private string _ignoreWarningPerIntegrationIdKey => $"{Constants.INNOVID_IGNORE_WARNING}_{CurrentIntegration.IntegrationID}";
    private IHttpClientProvider _httpClientProvider;

    private readonly BaseRepository<APIEntity> _apiEntityRepository = new BaseRepository<APIEntity>();

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;
        _apiClient = new(_httpClientProvider, base.CurrentCredential);
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, string.Format("{2} - {1}-IMPORT-PREEXECUTE {0}", "this.GetJobCacheKey()", this.CurrentSource, _jobGuid)));
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);

        SyncEntities();

        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID);

        _threadSleepDelay = int.Parse(SetupService.GetById<Lookup>(Constants.INNOVID_IMPORT_SLEEP).Value);
        _maxPendingReports = int.Parse(SetupService.GetById<Lookup>(Constants.INNOVID_MAX_PENDING_REPORTS).Value);
        _reports = JobService.GetAllActiveAPIReports<InnovidReportSettings>(base.SourceId);
        _remoteAccessClient = base.GetS3RemoteAccessClient();
        // using a scale of 100, lookup defines clients by a weight (percentage) and maybe a default weight to control the majority
        // EXAMPLE: if client A and client B each have a weight of 50, then they will comprise a whole bundle by themselves because a bundle totals 100
        var agencyBundleLookup = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.INNOVID_AGENCY_BUNDLES)?.Value) ? new List<WeightConfiguration>() : ETLProvider.DeserializeType<List<WeightConfiguration>>(SetupService.GetById<Lookup>(Constants.INNOVID_AGENCY_BUNDLES).Value);
        _weightConfigurations = agencyBundleLookup.Where(x => x.IsDefault != true).ToList();
        _defaultWeight = agencyBundleLookup.Find(x => x.IsDefault)?.Weight ?? 30;
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_DAILY_LOOKBACK)?.Value, out _dailyLookback))
            _dailyLookback = 2;
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_POLLY_MAX_RETRY)?.Value, out _maxRetry))
            _maxRetry = 13;
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_BREAK_RUNNING_CONSTANT)?.Value, out _break_constant))
        {
            _break_constant = 12000; //2 minutes 120000
        }

        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_BREAK_RUNNING_PER_REPORT)?.Value, out _break_per_report))
        {
            _break_per_report = 10000; //10 seconds
        }

        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_BREAK_STATUS_HOW_MANY_QUEUE)?.Value, out _check_report_status))
        {
            _check_report_status = 1; // allows 1 queue to not have a status change 
        }
        if (!TimeSpan.TryParse(SetupService.GetById<Lookup>(Constants.INNOVID_MAX_RUNTIME)?.Value, out _maxRuntime))
        {
            _maxRuntime = new TimeSpan(0, 3, 0, 0);
        }

        //initialize a new list of report items reserved for any cached report items not in this job's queue item list
        this._bundlesNotImportedThisTime = new List<ApiBundle>();

        var unfinishedReportsLookup = JobService.GetById<Lookup>(_unfinishedReportsKey);
        if (!string.IsNullOrEmpty(unfinishedReportsLookup?.Value))
        {
            this._unfinishedBundles = JsonConvert.DeserializeObject<List<ApiBundle>>(unfinishedReportsLookup.Value);
            this._bundlesNotImportedThisTime = this._unfinishedBundles.Where(r => !_queueItems.Any(q => q.ID == r.MainQueueId)).ToList();
            var queueIdList = JobService.GetQueueIDBySource(base.SourceId, CurrentIntegration.IntegrationID)?.ToList();
            this._bundlesNotImportedThisTime = this._bundlesNotImportedThisTime.Where(r => queueIdList.Contains(r.MainQueueId)).ToList();
        }
        else
        {
            this._unfinishedBundles = new List<ApiBundle>();
        }
    }

    public void Execute()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, string.Format("{1} - EXECUTE START {0}", this.GetJobCacheKey(), _jobGuid)));

        _runtime.Start();

        if (!_queueItems.Any())
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("No Queue to import")));
            return;
        }

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"START Creating bundles. Queues to Import: {_queueItems.Count()}")));
        List<ApiBundle> bundles = CreateBundles();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Total bundles created: {bundles.Count}|total cached bundles: {this._unfinishedBundles.Count}")));

        List<ApiReportItem> reportList = new List<ApiReportItem>();

        bundles.ForEach(bundle => reportList.AddRange(bundle.Reports));
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Adding reports -TOTAL reports: {reportList.Count}")));

        bool allReportsReady = false;
        int totalTrips = 0;
        do
        {
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) break;
            // reset any reports that have failed along the way at the onset
            // in order to re-submit their requests and get that queue to the finish line
            List<ApiReportItem> failedReports = reportList.Where(r => r.Status == ReportStatusType.FAIL).ToList();
            if (failedReports.Count != 0)
            {
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                    PrefixJobGuid($"There were failed reports! " +
                    $"No worries though, resetting flags and status to re-submit and get the Queue moving-" +
                    $"TOTAL failed: {failedReports.Count}" +
                    $"|failedReports:{string.Join(";", failedReports.Select(f => $"fileguid:{f.FileGuid}|name:{f.ReportName}"))}")));
                failedReports.ForEach(f => { f.Status = ReportStatusType.SUCCESS; f.IsReady = false; f.IsSubmitted = false; f.IsDownloaded = false; });
                _warningCounter++;
            }

            // set IsSubmitted to true for reports submitted to the API Run endpoint
            int reportSubmitted = SubmitReports(reportList);
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Reports Submitted={reportSubmitted}|total trips:{totalTrips}")));

            // take snapshot here after reports have been newly submitted
            SnapshotReports(reportList, bundles);

            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) break;
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Taking a break for {_break_constant} + ({reportSubmitted} reports * {_break_per_report} ) milliseconds.." +
                $"If this is too long for you, then change lookups INNOVID_BREAK_RUNNING_CONSTANT and INNOVID_BREAK_RUNNING_PER_REPORT")));
            Task.Delay(_break_constant + reportSubmitted * _break_per_report).Wait();
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) break;

            var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
            {
                Counter = 3,
                MaxRetry = _maxRetry,
                Seed = 60
            };

            var policy = new CancellableConditionalRetry<bool>(_jobGuid, apiCallsBackOffStrategy, _runtime, _maxRuntime, (bool anyReportReady) => anyReportReady == false);

            policy.Execute(() =>
            {
                // set IsReady to true when a report is ready or errored out
                int countReady = CheckStatusAndDownloadReport(reportList, bundles);
                allReportsReady = reportList.Count == 0 || reportList.All(x => x.IsReady == true);
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Reports Ready={countReady} - allReportReady={allReportsReady}|total trips:{totalTrips}")));

                // we exit poly if any report is ready and a new report can be submitted to run OR all reports are ready
                return countReady > 0 || allReportsReady;
            });

            // take snapshot here after reports become ready
            SnapshotReports(reportList, bundles);

            totalTrips++;
        } while (!allReportsReady);

        _runtime.Stop();

        SaveUnfinishedReports(reportList, bundles);

        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
        {
            _warningCounter++;
            _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                base.PrefixJobGuid(
                    $"Runtime exceeded time allotted - {_runtime.ElapsedMilliseconds}ms")));
        }

        if (this._exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {this._exceptionCount}; Please check Splunk for more detail.");
        }
        else if (_warningCounter > 0)
        {
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = $"Total warnings: {_warningCounter}; For full list search for Warnings in splunk";
        }
    }

    private List<ApiBundle> CreateBundles()
    {
        List<ApiBundle> bundles = new List<ApiBundle>();

        int currentWeight = 0;
        int bundleIndex = -1;

        // insert cached bundles that are in the queue
        List<long> queueIds = _queueItems.Select(q => q.ID).ToList();
        List<ApiBundle> unfinishedBundlesToProcess = this._unfinishedBundles.Where(r => queueIds.Contains(r.MainQueueId)).ToList();
        bundles.AddRange(unfinishedBundlesToProcess);
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Added {unfinishedBundlesToProcess.Count} Bundles that were saved in lookup. These will be skipped later..")));

        // get list of unfinished bundle IDs to check against and avoid duplicative bundle creation
        List<long> unfinishedBundleIds = this._unfinishedBundles?.SelectMany(u => u.QueueIds.Select(id => id)).Distinct().ToList() ?? new List<long>();

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"NOTE: a bundle can only hold 100 total weight and each entity by default weights a total of {_defaultWeight}. " +
            $"This can be configured in lookup called INNOVID_AGENCY_BUNDLES")));

        foreach (OrderedQueue queueItem in _queueItems.OrderBy(q => q.RowNumber))
        {
            // do not create a bundle cached in the unfinished lookup
            if (unfinishedBundleIds.Contains(queueItem.ID))
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"fileguid-{queueItem.FileGUID}|" +
                    $"entity:{queueItem.EntityID}|filedate:{queueItem.FileDate}|isbackfill{queueItem.IsBackfill}|" +
                    $"Skipping Bundle Creation for queue id={queueItem.ID} b/c it is exists in a cached bundle already")));
                continue;
            }

            bool canBundle = false;
            ApiBundle previousBundle = bundleIndex > -1 ? bundles[bundleIndex] : null;

            if (previousBundle != null)
            {
                var previousQueue = _queueItems.First(q => q.ID == previousBundle.MainQueueId);

                if (previousQueue.FileDate == queueItem.FileDate && previousQueue.IsBackfill == queueItem.IsBackfill)
                    canBundle = true;
            }

            int entityWeight = _weightConfigurations.Find(a => a.Bundles.Contains(queueItem.EntityID))?.Weight ?? _defaultWeight;

            // create a new bundle if current queue cannot join to previous one
            // or if the additional weight from the current queue exceeds the total allowed
            if (!canBundle || (currentWeight + entityWeight) > 100)
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    PrefixJobGuid($"fileguid-{queueItem.FileGUID}|entity:{queueItem.EntityID}|filedate:{queueItem.FileDate}|isbackfill{queueItem.IsBackfill}|" +
                    $"Creating Bundle for queue id={queueItem.ID}|canBundle:{canBundle}|currentWeight:{currentWeight}|entityWeight:{entityWeight}")));
                ApiBundle newBundle = new ApiBundle
                {
                    MainQueueId = queueItem.ID,
                    RowNumber = queueItem.RowNumber,
                    ClientIds = new List<string> { queueItem.EntityID },
                    QueueIds = new List<long> { queueItem.ID },
                    FileDate = queueItem.FileDate,
                    IsDaily = !queueItem.IsBackfill,
                    MainFileGuid = queueItem.FileGUID
                };

                newBundle.SetReports(queueItem, _reports, _dailyLookback);

                bundles.Add(newBundle);
                bundleIndex++;
                currentWeight = entityWeight;
                continue;
            }

            bundles[bundleIndex].QueueIds.Add(queueItem.ID);
            bundles[bundleIndex].ClientIds.Add(queueItem.EntityID);
            bundles[bundleIndex].SetReports(queueItem, _reports, _dailyLookback);
            currentWeight += entityWeight;

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"fileguid-{queueItem.FileGUID}|entity:{queueItem.EntityID}|filedate:{queueItem.FileDate}|isbackfill{queueItem.IsBackfill}|" +
                $"Added queue id={queueItem.ID} to bundle assigned under queue {bundles[bundleIndex].MainQueueId}({bundles[bundleIndex].MainFileGuid})" +
                $"|total queues:{bundles[bundleIndex].QueueIds.Count}|total clients:{bundles[bundleIndex].ClientIds.Count}|canBundle:{canBundle}|currentWeight:{currentWeight}|entityWeight:{entityWeight}")));
        }

        return bundles;
    }

    private void SyncEntities()
    {
        try
        {
            List<APIEntity> apiEntities = JobService.GetAllAPIEntities(sourceId: CurrentSource.SourceID, integrationID: CurrentIntegration.IntegrationID).ToList();
            List<IgnoreWarningSettings> apiEntitiesWithWarning = GetApiEntitiesWithWarning();
            ClientData innovidApiResult = GetAdvertisersAsync().GetAwaiter().GetResult();

            if (innovidApiResult?.Data?.Clients is null)
            {
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, base.PrefixJobGuid($"No entities returned from Innovid API")));
                return;
            }

            List<Client> innovidAccounts = innovidApiResult.Data.Clients;

            foreach (var apiEntity in apiEntities)
            {
                bool isDeactivated = !innovidAccounts.Any(acc => acc.ClientId.ToString().Trim().Equals(apiEntity.APIEntityCode.Trim(), StringComparison.OrdinalIgnoreCase));
                if (isDeactivated)
                {
                    DeactivateEntity(apiEntity);
                }
                else if (!apiEntity.IsActive && IsEntityWarningIgnored(apiEntitiesWithWarning, apiEntity))
                {
                    LogReactivationWarning(apiEntity);
                }
            }

            AddApiEntities(apiEntities, innovidAccounts);
        }
        catch (Exception ex)
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Error going through Entity Process Update from Innovid"), ex));
            _exceptionCount++;
        }
    }

    private void DeactivateEntity(APIEntity apiEntity)
    {
        _logger.Log(Msg.Create(LogLevel.Info,
                                _logger.Name,
                                PrefixJobGuid($"API Entity with Code : {apiEntity.APIEntityCode} wasn't found in Innovid's response " +
                                $"and therefore it will be deactivated.")));

        apiEntity.IsActive = false;
        apiEntity.LastUpdated = DateTime.UtcNow;
        JobService.Update(apiEntity);
    }

    private void LogReactivationWarning(APIEntity apiEntity)
    {
        _logger.Log(Msg.Create(LogLevel.Warn,
                                _logger.Name,
                                PrefixJobGuid($"" +
                                $"The deactivated API Entity with Code : {apiEntity.APIEntityCode} was found in Innovid's response. " +
                                $"Do you want to reactivate it?")));
        _warningCounter++;
    }

    private static bool IsEntityWarningIgnored(List<IgnoreWarningSettings> apiEntitiesWithWarning, APIEntity apiEntity)
    {
        return !apiEntitiesWithWarning.Exists(apiEntityWarning => apiEntityWarning.ApiEntityCode.Trim().Equals(apiEntity.APIEntityCode.Trim(), StringComparison.OrdinalIgnoreCase));
    }

    private void AddApiEntities(IEnumerable<APIEntity> apiEntities, List<Client> innovidAccounts)
    {
        IEnumerable<Client> newAccounts = innovidAccounts.Where(acc =>
            !apiEntities.Any(entity => entity.APIEntityCode.Trim().Equals(acc.ClientId.ToString().Trim(), StringComparison.OrdinalIgnoreCase)));

        if (!newAccounts.Any())
        {
            _logger.Log(Msg.Create(
                     LogLevel.Info,
                     _logger.Name,
                     $"Innovid - No entities to create."));

            return;
        }

        var entitiesToCreate = CreateAPIEntities(newAccounts);
        _logger.Log(Msg.Create(
               LogLevel.Info,
               _logger.Name,
               $"Innovid - {entitiesToCreate.Count} Entities to create."));

        foreach (var apiEntity in entitiesToCreate)
        {
            try
            {
                _apiEntityRepository.Add(apiEntity);
            }
            catch (Exception ex)
            {
                _logger.Log(Msg.Create(
                    LogLevel.Error,
                    _logger.Name,
                    base.PrefixJobGuid($"Innovid : Error adding an API Entity in the DB. APIEntityCode : {apiEntity.APIEntityCode}"), ex));
            }
        }
    }

    private async Task<ClientData> GetAdvertisersAsync()
    {
        return await _apiClient.GetAdvertisersAsync(new GetAdvertisersOptions()
        {
            UrlExtension = "advertisers",
        });
    }

    private List<IgnoreWarningSettings> GetApiEntitiesWithWarning()
    {
        var ignoreWarningLookup = JobService.GetById<Lookup>(_ignoreWarningPerIntegrationIdKey);

        if (ignoreWarningLookup == null)
        {
            return new List<IgnoreWarningSettings>();
        }

        InnovidIgnoreWarningConfig warningConfig = JsonConvert.DeserializeObject<InnovidIgnoreWarningConfig>(ignoreWarningLookup.Value);
        return warningConfig?.APIEntitiesWithWarningList ?? new List<IgnoreWarningSettings>();
    }

    private List<APIEntity> CreateAPIEntities(IEnumerable<Client> accounts)
    {
        var entitiesToCreate = new List<APIEntity>();

        foreach (var account in accounts)
        {
            var apiEntity = new APIEntity
            {
                APIEntityCode = account.ClientId.ToString().Trim(),
                APIEntityName = account.Name,
                SourceID = CurrentSource.SourceID,
                IntegrationID = CurrentIntegration.IntegrationID,
                BackfillPriority = false,
                CreatedDate = DateTime.UtcNow,
                LastUpdated = DateTime.UtcNow,
                IsActive = true,
                StartDate = DateTime.Now,
                TimeZone = "Eastern Standard Time"
            };

            entitiesToCreate.Add(apiEntity);
        }

        return entitiesToCreate;
    }

    private int SubmitReports(List<ApiReportItem> reportList)
    {
        int reportSubmitted = 0;

        if (reportList.Count == 0)
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid("There are no reports to run")));
            return 0;
        }

        foreach (var reportItem in reportList.Where(r => !r.IsSubmitted).OrderBy(r => r.PriorityNumber))
        {
            int runningReports = reportList.Count(r => r.IsSubmitted && !r.IsReady);

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Running Reports:{runningReports} max({_maxPendingReports})")));

            //check if we reached the maximum of running reports
            if (runningReports >= _maxPendingReports)
            {
                return reportSubmitted;
            }

            if (TimeSpan.Compare(this._runtime.Elapsed, this._maxRuntime) == 1)
            {
                //the runtime is greater than the max RunTime
                return reportSubmitted;
            }

            try
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(
                    $"Innovid Report {reportItem.ReportName}-{reportItem.ReportType} FileGUID: {reportItem.FileGuid} -Submitting Report..")));

                var options = new RequestReportOptions()
                {
                    Content = reportItem.ReportRequestBody,
                    UrlExtension = "reports",
                };

                var reportRequest = _apiClient.RequestReportAsync(options).GetAwaiter().GetResult();

                if (reportRequest == null || UtilsText.ConvertToEnum<ReportStatusType>(reportRequest.status) != ReportStatusType.SUCCESS)
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(
                        $"Innovid Report {reportItem.ReportName}-{reportItem.ReportType} " +
                        $"Submitted but response was not successful- Exiting report submission step - " +
                        $"FileGUID: {reportItem.FileGuid} -" +
                        $"Report Response Status->{reportRequest?.status} " +
                        $"ReportID->{reportRequest?.data}")));
                    _warningCounter++;
                    return reportSubmitted;
                }

                reportItem.ReportID = reportRequest.data;
                reportItem.IsSubmitted = true;
                reportItem.IsReady = false;
                reportItem.Status = UtilsText.ConvertToEnum<ReportStatusType>(reportRequest.status);
                reportItem.TimeSubmitted = DateTime.UtcNow;

                bool allBundledReportsSubmitted = reportList.Where(x => x.QueueID == reportItem.QueueID).All(x => x.IsSubmitted == true);
                if (allBundledReportsSubmitted)
                    JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Running);
                reportSubmitted++;

                // delay between requests
                Task.Delay(300).Wait();
            }
            catch (HttpClientProviderRequestException hex)
            {
                HandleException(reportItem, hex);
            }
            catch (Exception exc)
            {
                HandleException(reportItem, exc);
            }
        }

        return reportSubmitted;
    }

    private void HandleException<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        this._exceptionCount++;
        reportItem.Status = ReportStatusType.FAIL;
        var logMsg = BuildLogMessage(reportItem, exc);
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(logMsg), exc));
    }

    private static string BuildLogMessage<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Error Running report - failed on queueID : {reportItem.QueueID} " +
                           $"for FileGUID : {reportItem.FileGuid} " +
                           $"Report Name : {reportItem.ReportName} -> " +
                           $"Exception details : {httpEx}",
            _ =>
                $"Error Running report - failed on queueID: {reportItem.QueueID} for fileguid: {reportItem.FileGuid} " +
                $"Report Name: {reportItem.ReportName} - Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }

    private int CheckStatusAndDownloadReport(List<ApiReportItem> reportList, List<ApiBundle> bundles)
    {
        int countReady = 0;
        int countNoChange = 0;
        ReportStatusData reportData = null;

        if (reportList.Count == 0)
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid("There are no reports to check status or download")));
            return countReady;
        }

        var reports = reportList.Where(x => x.IsSubmitted && (!x.IsReady || !x.IsDownloaded)).ToList();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid($"Start checking status - total reports to check: {reports.Count}")));

        foreach (var reportItem in reports.OrderBy(q => q.PriorityNumber).ThenBy(r => r.TimeSubmitted))
        {
            try
            {
                var queueItem = _queueItems.FirstOrDefault(x => x.ID == reportItem.QueueID);
                var reportURI = $"reports/{reportItem.ReportID}/status";
                var options = new GetReportStatusOptions()
                {
                    UrlExtension = reportURI,
                    PropertyName = "reportStatus"
                };

                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                    PrefixJobGuid(
                        $"START Innovid Check Report Status {reportItem.ReportName}-{reportItem.ReportType}: " +
                        $"FileGUID: {reportItem.FileGuid} " +
                        $"date-> {reportItem.Date}->URI:{reportURI}.")));

                reportData = _apiClient.GetReportStatusAsync(options).GetAwaiter().GetResult();

                if (reportData == null)
                    continue;

                reportItem.Status = Utilities.UtilsText.ConvertToEnum<ReportStatusType>(reportData?.data?.reportStatus);

                ReportStatusType reportStatus = UtilsText.ConvertToEnum<ReportStatusType>(reportData.status);
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(
                    $"FileGUID: {reportItem.FileGuid}-" +
                    $"Innovid Report status is {reportItem.Status} " +
                    $"date-> {reportItem.Date}->{reportItem.ReportName}-{reportItem.ReportType}->" +
                    $"callStatus:{reportStatus}->.")));

                if (reportItem.Status == ReportStatusType.READY)
                {
                    reportItem.IsReady = true;
                    reportItem.ReportURL = reportData.data.reportUrl;

                    reportItem.IsDownloaded = DownloadReport(reportItem);
                    countReady++;

                    if (reportItem.IsDownloaded)
                    {
                        bool allReportsForQueueDownloaded = reportList.Where(x => x.QueueID == reportItem.QueueID)
                                            .All(x => x.IsDownloaded == true && x.IsReady == true);
                        if (allReportsForQueueDownloaded)
                        {
                            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"" +
                                $"FileGUID: {reportItem.FileGuid}- " +
                                $"ALL reports have been downloaded for Date={queueItem.FileDate}. Yay!!!")));

                            ApiBundle queueBundle = bundles.First(b => b.MainQueueId == queueItem.ID);
                            List<ApiReportItem> bundleReports = reportList.Where(x => x.QueueID == reportItem.QueueID).ToList();
                            var fileCollection = bundleReports.ConvertAll(r => r.FileCollectionItem);
                            var fileSize = fileCollection.Sum(f => f.FileSize);

                            List<long> queuesToDelete = queueBundle.QueueIds.Except(new List<long> { queueBundle.MainQueueId }).ToList();

                            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Innovid start updating to Import Complete-FileGUID: {reportItem.FileGuid} queueID: {queueItem.ID}")));
                            queueItem.FileCollectionJSON = (fileCollection != null) ? JsonConvert.SerializeObject(fileCollection) : "";
                            queueItem.FileSize = fileSize;
                            queueItem.Status = Constants.JobStatus.Complete.ToString();
                            queueItem.StatusId = (int)Constants.JobStatus.Complete;
                            queueItem.Step = Constants.JobStep.Import.ToString();
                            JobService.Update((Queue)queueItem);

                            if (queuesToDelete.Count != 0)
                            {
                                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Update bundled Queues -FileGUID: {reportItem.FileGuid}- The following queues were imported successfully and deleted as 1 queue will contain all reports. Deleting queue IDs={string.Join(", ", queuesToDelete.Select(id => id))}")));
                                UpdateQueueWithDelete(queuesToDelete.Select(id => new Queue { ID = id }), Constants.JobStatus.Complete, true);
                            }

                            bundles.Remove(queueBundle);
                            reportList.RemoveAll(r => r.QueueID == queueItem.ID);
                        }
                    }
                    else  //Error queue Item, if any of its report types failed to download
                    {
                        reportItem.Status = ReportStatusType.FAIL;
                        reportItem.IsReady = true;
                        countReady++;
                    }
                }
                else if (reportItem.Status == ReportStatusType.FAIL) /* Error out Queue */
                {
                    _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                        PrefixJobGuid(
                            $"Innovid ReportURIstatus is not successful. Will re-submit failed report on next turn:FileGUID: {reportItem.FileGuid} date-> {reportItem.Date}->{reportItem.ReportName}-{reportItem.ReportType}->status:{reportItem.Status}.")));

                    reportItem.IsReady = true;
                    countReady++;
                }
                else
                {
                    // if the status has not changed stop checking the status and wait. The goal is to save on the number of api calls made
                    // we allow a number of check_report_status reports without a status change before stopping
                    if (++countNoChange > _check_report_status)
                    {
                        return countReady;
                    }
                }

                ThreadSleep();
            }
            catch (HttpClientProviderRequestException hex)
            {
                HandleExceptionWhenCheckingStatus(reportItem, hex);
                countReady++;
            }
            catch (Exception exc)
            {
                HandleExceptionWhenCheckingStatus(reportItem, exc);
                countReady++;
            }//end try catch
        } //end for

        return countReady;
    }

    private void HandleExceptionWhenCheckingStatus<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        this._exceptionCount++;
        reportItem.Status = ReportStatusType.FAIL;
        reportItem.IsReady = true;

        var logMsg = BuildLogMessageWhenCheckingStatus(reportItem, exc);

        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(logMsg), exc));
    }
    private static string BuildLogMessageWhenCheckingStatus<TException>(ApiReportItem reportItem, TException ex) where TException : Exception
    {
        return ex switch
        {
            HttpClientProviderRequestException httpEx => $"Error checking report status- failed on queueID : {reportItem.QueueID} " +
                           $"for FileGUID : {reportItem.FileGuid} " +
                           $"Report Name : {reportItem.ReportName} -> " +
                           $"Exception details : {httpEx}"
                ,
            _ => $"Error checking report status- failed on queueID: {reportItem.QueueID} " +
            $"for FileGUID: {reportItem.FileGuid} Report Name: {reportItem.ReportName} - Exception: {ex.Message} - STACK {ex.StackTrace}"

        };
    }
    private void SaveUnfinishedReports(List<ApiReportItem> reportList, List<ApiBundle> bundles)
    {
        var failedReports = reportList
            .Where(g => g.Status == ReportStatusType.FAIL);
        var failedQueueIDs = failedReports.Select(x => x.QueueID).Distinct();

        if (failedQueueIDs.Any())
            base.UpdateQueueWithDelete(failedQueueIDs.Select(id => new Queue { ID = id }), Common.Constants.JobStatus.Error, false);

        // let's clean up the snapshot with the final save
        SnapshotReports(reportList, bundles, true);
    }

    private void SnapshotReports(List<ApiReportItem> reportList, List<ApiBundle> bundles, bool cleanupSnapshot = false)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"START taking snapshot of unfinished reports in Lookup {_unfinishedReportsKey}..")));
        //building the list of reports to save in lookup
        var unfinishedBundles = new List<ApiBundle>();

        List<long> submittedQueueId = reportList.Where(r => r.IsSubmitted == true).Select(x => x.QueueID).Distinct().ToList();
        List<ApiBundle> runningBundles = bundles.Where(b => submittedQueueId.Contains(b.MainQueueId)).ToList();

        unfinishedBundles.AddRange(runningBundles);
        unfinishedBundles.AddRange(this._bundlesNotImportedThisTime); // adding all the reports that where not part of the selection of queues (SP GetActiveOrderedTopQueueItemsBySource: Select top X from queue...)

        // cleanup: remove any reports associated with a queue ID that is not in the Queue Table anymore
        if (cleanupSnapshot)
        {
            var queueIdList = JobService.GetQueueIDBySource(base.SourceId, CurrentIntegration.IntegrationID)?.ToList();
            unfinishedBundles = unfinishedBundles.Where(r => queueIdList.Contains(r.MainQueueId)).ToList();
        }

        var lookup = new Lookup
        {
            Name = _unfinishedReportsKey,
            Value = JsonConvert.SerializeObject(unfinishedBundles),
            LastUpdated = DateTime.Now
        };

        Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
        Data.Repositories.LookupRepository.AddOrUpdateLookup(lookup);

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Stored unfinished reports in Lookup {_unfinishedReportsKey}-total bundles: {unfinishedBundles.Count}")));
    }


    private bool DownloadReport(ApiReportItem reportItem)
    {
        bool returnVal;
        try
        {
            string fileName = $"{reportItem.FileGuid}_{reportItem.ReportName}.zip";

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(
                $"Innovid start GetReport-FileGUID: {reportItem.FileGuid}: date-> {reportItem.Date}->{reportItem.ReportType}->URL:{reportItem.ReportURL}. Saving to S3 as {fileName}")));

            var options = new DownloadReportOptions()
            {
                UriPath = reportItem.ReportURL,
            };

            var response = _apiClient.DownloadReportAsync(options).GetAwaiter().GetResult();

            using (MemoryStream memoryStream = new MemoryStream())
            using (Stream webFileStream = response)
            {
                //DIAT-15556: We are using one ZipArchive to read from and another to write renamed files to
                using (var newArchive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                using (var archive = new ZipArchive(webFileStream))
                {
                    var entries = archive.Entries.ToArray();
                    foreach (var entry in entries)
                    {
                        //DIAT-15556: We must manually rename the CSV files contained within the Zip file downloaded from Innovid.
                        //This is because we want the files uploaded to S3 to contain our report types (i.e. version_delivery),
                        //but Innovid only recognizes two types: deliverability and viewability. We will replace Innovid's report type in the file name with our own.
                        var name = entry.Name;
                        string[] splitName = name.Split('_');
                        splitName[1] = reportItem.ReportName;
                        var newName = string.Join("_", splitName);

                        var newEntry = newArchive.CreateEntry(newName);
                        using (var oldEntryStream = entry.Open())
                        using (var newEntryStream = newEntry.Open())
                        {
                            oldEntryStream.CopyTo(newEntryStream);
                        }
                    }
                }

                memoryStream.Seek(0, SeekOrigin.Begin);

                string[] paths = new string[]
                {
                    GetDatedPartition(reportItem.Date), fileName
                };

                var incomingFile = new StreamFile(memoryStream, GreenhouseS3Creds);
                S3File rawFile = new S3File(RemoteUri.CombineUri(this._baseDestUri, paths),
                    GreenhouseS3Creds);
                base.UploadToS3(incomingFile, rawFile, paths);

                reportItem.FileCollectionItem = new FileCollectionItem()
                {
                    SourceFileName = reportItem.ReportType,
                    FileSize = rawFile.Length,
                    FilePath = fileName
                };
            }

            returnVal = true;
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"Innovid end GetReport:FileGUID: {reportItem.FileGuid} date-> {reportItem.Date}->{reportItem.ReportType}. Saved to S3 as {fileName}")));
        }
        catch (HttpClientProviderRequestException hex)
        {
            returnVal = HandleExceptionWhenDownloadReport(reportItem, hex);
        }
        catch (Exception exce)
        {
            returnVal = HandleExceptionWhenDownloadReport(reportItem, exce);
        }

        return returnVal;
    }

    private bool HandleExceptionWhenDownloadReport<TException>(ApiReportItem reportItem, TException exception) where TException : Exception
    {
        bool returnVal = false;
        _exceptionCount++;

        var logMsg = BuildLogMessageWhenDownloadReport(reportItem, exception);

        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(logMsg), exception));
        return returnVal;
    }

    private static string BuildLogMessageWhenDownloadReport<TException>(ApiReportItem reportItem, TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"Innovid HttpRequestException download failed " +
            $"FileGUID: {reportItem.FileGuid} | Exception details: {httpEx}",

            _ => $"Innovid exception download failed: Error message: {exception.Message}"
        };
    }

    /// <summary>
    /// Innovid API documentation suggests there should be a 30 second delay between calls, as best practice.
    /// </summary>
    private void ThreadSleep(int? defaultSleepMS = null)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("Putting thread to sleep for 30seconds before next request as per API documentation.")));
        var jobDelay = Task.Run(async () => await Task.Delay(defaultSleepMS ?? _threadSleepDelay));
        jobDelay.Wait();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("Thread 30 seconds is Complete")));
        return;
    }

    void IDragoJob.PostExecute() { }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _remoteAccessClient?.Dispose();
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }

    public string GetJobCacheKey()
    {
        return DefaultJobCacheKey;
    }
}

using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Snapchat;
using Greenhouse.Data.DataSource.Snapchat;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Model.Snapchat;
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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using TimeZoneConverter;

namespace Greenhouse.Jobs.Aggregate.SnapchatAds;

[Export("SnapchatAds-AggregateImportJob", typeof(IDragoJob))]
public partial class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient remoteAccessClient;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private Uri baseLocalRawUri;
    private List<IFileItem> queueItems;
    private IEnumerable<Greenhouse.Data.Model.Aggregate.APIReport<ReportSettings>> _reports;
    private string JobGUID => base.JED.JobGUID.ToString();
    private SnapchatProvider snapchatProvider;
    private JsonSerializerSettings redshiftSerializerSettings;
    private ICollection<ReportState> snapchatReportState;
    private ReportState currentReportState;
    private ReportState staticDimsReportState;
    private ConcurrentBag<FileCollectionItem> allCreatedFiles;
    private readonly List<string> MetricReports = new List<string> { "stats", "conversions" };
    private TimeSpan maxRuntime;
    private readonly Stopwatch _runTime = new();
    private int mappingRecordsPerFile;
    private IEnumerable<APIEntity> APIEntities;
    private int nbDayBuffer;
    private List<string> campaignDailyStatus;
    private int _maxRetry;
    private readonly static CompositeFormat _snapchat_account_prefix = CompositeFormat.Parse(Constants.SNAPCHAT_ACCOUNT_PREFIX);
    private WhiteListingSettings _creativeWhiteListingSettings;

    // used for dimension reports :if the report.settings has a value set for the
    // property ParentDataPath this means that some data needs to be extracted
    // to be used for other reports that have that Report Entity as parent
    // ParentDataPath is the json path to the data to cache
    //
    // Those cached values are then used with reports that have the property ParentEntity
    // set to the Report name that cached values
    //
    // example: the Country Report has ParentDataPath set to the js path of country code in the API response
    // and Region Report has ParentEntity=Country so it will loop through country codes to retrieve all regions
    private Dictionary<string, List<string>> _parentCache;
    private Dictionary<string, List<APIReport<Data.DataSource.Snapchat.ReportSettings>>> _attributionReports;
    private IHttpClientProvider _httpClientProvider;
    private int _maxDegreeOfParallelism;
    private int _maxRequestsPerWindow;
    private int _apiRequestTimeWindow;
    private int _exceptionCounter;
    // by using a static property, all instances ( = all integrations) will share a reference to the same object
    // using a lock on that object means that only 1 instance at a time can execute the code within the lock
    private static readonly object _classLock = new object();
    private readonly QueueServiceThreadLock _queueServiceThreadLock = new QueueServiceThreadLock(_classLock);

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        // temp destinations on local drive
        baseLocalRawUri = GetLocalImportDestinationFolder();

        // final destinations on S3
        baseRawDestUri = GetDestinationFolder();
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(nameof(Constants.ProcessingStage.RAW).ToLower(), nameof(Constants.ProcessingStage.STAGE).ToLower()));
        LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        int parentIntegrationID = CurrentIntegration.ParentIntegrationID ?? CurrentIntegration.IntegrationID;
        queueItems = _queueServiceThreadLock.GetOrderedTopQueueItemsByCredential(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.CredentialID, parentIntegrationID)?.ToList();

        remoteAccessClient = base.GetS3RemoteAccessClient();
        _reports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_BACKOFF_MAX_RETRY, 10);

        redshiftSerializerSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.None
        };
        redshiftSerializerSettings.Converters.Add(new Greenhouse.Utilities.IO.RedshiftConverter());

        // retrieves the Snapchat State from the Lookup table
        // this object contains, for each AccountId: the DeltaDate (last date the dimension reports were pulled)
        snapchatReportState = LookupService.GetAndDeserializeLookupValueWithDefault<ICollection<ReportState>>($"{Constants.SNAPCHAT_STATE}_{CurrentIntegration.IntegrationID}", new List<ReportState>());

        string creativeWhitelistingValue = LookupService.GetLookupValueWithDefault($"{Constants.SNAPCHAT_CREATIVE_WHITELISTING}_{parentIntegrationID}", null);
        if (string.IsNullOrEmpty(creativeWhitelistingValue))
        {
            _creativeWhiteListingSettings = new WhiteListingSettings { AllowAll = false, WhiteList = new() };
        }
        else
        {
            _creativeWhiteListingSettings = ETLProvider.DeserializeType<WhiteListingSettings>(creativeWhitelistingValue);
        }

        _parentCache = new Dictionary<string, List<string>>();

        maxRuntime = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));

        //attribution window saved in lookup are managed by Product team
        var attributionWindowLookup = LookupService.GetAndDeserializeLookupValueWithDefault<List<AttributionWindowLookup>>(Constants.SNAPCHAT_ATTRIBUTION_WINDOWS, new List<AttributionWindowLookup>());
        var attributionReportLookup = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.SNAPCHAT_ATTRIBUTION_REPORTS)?.Value)
            ? new List<string>()
            : SetupService.GetById<Lookup>(Constants.SNAPCHAT_ATTRIBUTION_REPORTS).Value.Split(',').ToList();
        _attributionReports = GetAttributionReports(attributionWindowLookup, attributionReportLookup);

        mappingRecordsPerFile = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_MAPPINGS_PER_FILE, 3000);

        APIEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID);

        nbDayBuffer = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_NB_DAY_BUFFER, 8);

        campaignDailyStatus = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.SNAPCHAT_REPORTS_CAMPAIGN_DAILY_STATUS)?.Value) ? new List<string> { "ACTIVE" } : SetupService.GetById<Lookup>(Constants.SNAPCHAT_REPORTS_CAMPAIGN_DAILY_STATUS).Value.Split(',').ToList();

        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_MAX_DEGREE_OF_PARALLELISM, 5);
        _apiRequestTimeWindow = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_API_LIMIT_TIME_WINDOW_MILLISECONDS, 1000);
        _maxRequestsPerWindow = LookupService.GetLookupValueWithDefault(Constants.SNAPCHAT_API_LIMIT_MAX_REQUESTS_PER_WINDOW, 20);

        snapchatProvider = new SnapchatProvider(_httpClientProvider, CurrentCredential, GreenhouseS3Creds, JobGUID);
    }

    private Dictionary<string, List<APIReport<ReportSettings>>> GetAttributionReports(List<AttributionWindowLookup> attributionWindowLookup
        , List<string> attributionReportLookup)
    {
        var attributionReportDictionary = new Dictionary<string, List<APIReport<ReportSettings>>>();

        var cleanReportNames = attributionReportLookup.Select(reportName
            => CleanReportNamesRegex().Replace(reportName, ""));
        // find attribution reports by matching report name in lookup to api report name in table
        var attributionReports = _reports.Where(x => cleanReportNames.Any(r => r.Equals(x.APIReportName, StringComparison.InvariantCultureIgnoreCase))).ToList();

        foreach (var lookup in attributionWindowLookup)
        {
            var newAttributionReports = new List<APIReport<ReportSettings>>();
            var distinctWindows = lookup.Windows.Select(x => new { x.SwipeWindow, x.ViewWindow }).Distinct();
            foreach (var attributionWindow in distinctWindows)
            {
                var newReports = Greenhouse.Utilities.UtilsIO.DeepCloneJson(attributionReports);
                newReports.ForEach(x =>
                {
                    x.ReportSettings.SwipeUpAttributionWindow = attributionWindow.SwipeWindow;
                    x.ReportSettings.ViewAttributionWindow = attributionWindow.ViewWindow;
                });

                if (!attributionReportDictionary.TryGetValue(lookup.EntityID, out newAttributionReports))
                {
                    newAttributionReports = attributionReportDictionary[lookup.EntityID] = new List<APIReport<ReportSettings>>();
                }

                newAttributionReports.AddRange(newReports);
            }
        }

        return attributionReportDictionary;
    }

    public void Execute()
    {
        var importDateTime = DateTime.UtcNow;
        _runTime.Start();

        if (queueItems.Count == 0)
        {
            LogMessage(LogLevel.Info, "There are no reports in the Queue");
            return;
        }
        // static dim reports are independent from any adaccount info, for example: countries, phone types
        // they only need to be generated once a day
        // once a queue is successfully created, the current date is stored in the Lookup table (snapchatReportState)
        if (snapchatReportState.All(s => s.AccountId != Constants.SNAPCHAT_STATIC_DIMS_KEY))
        {
            snapchatReportState.Add(new ReportState { AccountId = Constants.SNAPCHAT_STATIC_DIMS_KEY });
        }

        staticDimsReportState = snapchatReportState.First(s => s.AccountId == Constants.SNAPCHAT_STATIC_DIMS_KEY);

        foreach (IFileItem queueItem in queueItems)
        {
            if (TimeSpan.Compare(_runTime.Elapsed, maxRuntime) == 1)
            {
                JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
                JobLogger.JobLog.Message = "Job RunTime exceeded max runtime.";
                break;
            }

            allCreatedFiles = new ConcurrentBag<FileCollectionItem>();

            var reportItems = new List<ApiReportItem>();

            LogMessage(LogLevel.Info, $"Queue fileguid={queueItem.FileGUID} - Start");

            queueItem.Status = nameof(Constants.JobStatus.Running);
            queueItem.StatusId = (int)Constants.JobStatus.Running;
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
            queueItem.FileCollectionJSON = null;
            queueItem.FileSize = 0;

            // map additional info (profileID) to the requestID from the raw file
            // this will be saved in a file so etl can associate the additional info
            // to a file
            var mappingFile = new List<Mapping>();

            try
            {
                LogMessage(LogLevel.Info, $"Queue fileguid={queueItem.FileGUID} - Get Dimension");
                GetDimensionReports(reportItems, queueItem, importDateTime, mappingFile);

                LogMessage(LogLevel.Info, $"Queue fileguid={queueItem.FileGUID} - Get Reports");
                GetMeasurementReports(reportItems, queueItem, mappingFile);

                LogMessage(LogLevel.Info, $"Queue fileguid={queueItem.FileGUID} - Finalize");
                FinalizeQueue(reportItems, queueItem, importDateTime, mappingFile);
            }
            catch (HttpClientProviderRequestException httpEx)
            {
                HandleException(queueItem, reportItems, httpEx);
                continue;
            }
            catch (Exception exc)
            {
                HandleException(queueItem, reportItems, exc);
                continue;
            }

            LogMessage(LogLevel.Info, $"Queue fileguid={queueItem.FileGUID} - End");
        }

        if (_exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCounter}; Please check Splunk for more detail.");
        }

        LogMessage(LogLevel.Info, "Import job complete");
    }

    private void HandleException<TException>(IFileItem queueItem, List<ApiReportItem> reportItems, TException ex)
        where TException : Exception
    {
        _exceptionCounter++;
        queueItem.Status = Constants.JobStatus.Error.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        // Build log message
        var logMessage = BuildLogMessage(queueItem, ex);

        logger.Log(Msg.Create(LogLevel.Error, logger.Name, base.PrefixJobGuid(logMessage), ex));
        // all or nothing: if a report fails, we remove all the other reports for that queue
        // and break the report loop to not go through the following reports
        reportItems.RemoveAll(x => x.QueueID == queueItem.ID);
    }

    private static string BuildLogMessage<TException>(IFileItem queueItem, TException ex) where TException : Exception
    {
        return ex switch
        {
            HttpClientProviderRequestException httpEx =>
                $"HttpClientProviderRequestException Error while processing queue item: {queueItem.FileGUID} for EntityID: {queueItem.EntityID}. " +
                                         $"Exception details : {httpEx}",
            _ =>
                $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> Exception: {ex.Message} - STACK {ex.StackTrace}"
        };
    }

    private string GetParameters(APIReport<ReportSettings> report)
    {
        var parameters = "";
        bool avoidPageLimit = false;

        if (MetricReports.Any(r => report.ReportSettings.ReportType.Equals(r, StringComparison.InvariantCultureIgnoreCase)))
        {
            var fields = report.ReportFields.Select(s => s.APIReportFieldName)
                    .Aggregate((current, next) => current + "," + next);

            if (!string.IsNullOrEmpty(fields))
            {
                parameters += $"&fields={fields}";
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.Granularity))
            {
                parameters += $"&granularity={report.ReportSettings.Granularity}";
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.Breakdown))
            {
                parameters += $"&breakdown={report.ReportSettings.Breakdown}";
            }
            else
            {
                //adding condition here to skip adding page limit when there is no breakdown parameter
                //otherwise, API will throw below error
                //"error_code": "E1008"
                //"Unsupported Stats Query: Pagination limit can only be used with breakdown query."
                avoidPageLimit = true;
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.ConversionSourceTypes))
            {
                parameters += $"&conversion_source_types={report.ReportSettings.ConversionSourceTypes}";
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.SwipeUpAttributionWindow))
            {
                parameters += $"&swipe_up_attribution_window={report.ReportSettings.SwipeUpAttributionWindow}";
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.ViewAttributionWindow))
            {
                parameters += $"&view_attribution_window={report.ReportSettings.ViewAttributionWindow}";
            }

            parameters += $"&omit_empty={report.ReportSettings.OmitEmpty}";

            if (!string.IsNullOrEmpty(report.ReportSettings.ReportDimension))
            {
                parameters += $"&report_dimension={report.ReportSettings.ReportDimension}";
            }
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.Limit) && !avoidPageLimit)
        {
            parameters += $"&limit={report.ReportSettings.Limit}";
        }

        return parameters;
    }

    private void GetDimensionReports(List<ApiReportItem> reportItems, IFileItem queueItem, DateTime importDateTime, List<Mapping> mappingFile)
    {
        // keeping track of the total number of dim report type we are downloading
        // based on the status dates saved in Lookup we will download or not static dims and regular dims

        _parentCache = new Dictionary<string, List<string>>();

        var dimensionReports = _reports.Where(r => r.ReportSettings.ReportType.Equals("dimension") && r.IsActive);

        //ApiEntity will include Organization ID
        var orgId = queueItem.EntityID.ToLower();

        if (snapchatReportState.All(s => s.AccountId != orgId))
        {
            snapchatReportState.Add(new ReportState { AccountId = orgId });
        }

        currentReportState = snapchatReportState.First(s => s.AccountId == orgId);

        //if deltadate is null, then we force the job to get dimension data by setting the report-state-date to a lesser date than now
        var reportStateDate = currentReportState.DeltaDate == null ? DateTime.UtcNow.AddDays(-2) : currentReportState.DeltaDate.Value.Date;
        if (importDateTime.Date > reportStateDate.Date)
        {
            //Static Dimensions = not dependent on the queue.EntityID (Countries, Phone types...)
            StaticDimReports(reportItems, (Queue)queueItem, dimensionReports);

            //Dimensions dependent on queue.EntityID (Campaign, AdSquad...)
            RegularDimReports(reportItems, (Queue)queueItem, orgId, reportStateDate, dimensionReports, mappingFile);
        }
        else
        {
            LogMessage(LogLevel.Info, "Skipping dimension download");
        }
    }

    private void RegularDimReports(List<ApiReportItem> reportItems, Queue queueItem, string orgId, DateTime reportStateDate,
        IEnumerable<APIReport<ReportSettings>> dimensionReports, List<Mapping> mappingFile)
    {
        var adAccounts = GetAdAccounts(reportItems, queueItem);

        if (adAccounts.Count == 0)
        {
            throw new APIReportException($"No accounts found for organization ID {orgId}.");
        }

        LogMessage(LogLevel.Debug, $"Getting dimension reports for account: {orgId}; file date: {queueItem.FileDate}; " +
            $"fileGUID: {queueItem.FileGUID}; Dimension reports last pulled on: {reportStateDate.Date}; Total accounts: {adAccounts.Count}");

        var regularDimReports = dimensionReports
            .Where(d => !d.ReportSettings.IsStatic)
            .OrderBy(d => d.ReportSettings.ParentEntity != null); // report with no parent entity first

        //account data was already downloaded during retrieval so we exclude it here (call to method GetAddAccount)
        var dimReportsFiltered = regularDimReports.Where(x => !x.ReportSettings.Entity.Equals("AdAccounts"));

        var accountIdList = adAccounts.Select(x => x.Key).ToList();

        if (!_parentCache.TryAdd("adaccounts", accountIdList))
        {
            _parentCache["adaccounts"].AddRange(accountIdList);
        }

        SendDimRequests(reportItems, queueItem, dimReportsFiltered, mappingFile, true);
    }

    private void StaticDimReports(List<ApiReportItem> reportItems, Queue queueItem, IEnumerable<APIReport<ReportSettings>> dimensionReports)
    {
        // if static dimension reports haven't been generated for Today's date, we generated them
        // otherwise we generate placeholders
        var staticDimReports = dimensionReports.Where(d => d.ReportSettings.IsStatic)
            .OrderBy(d => d.ReportSettings.ParentEntity != null); // report with no parent entity first

        if (!staticDimsReportState.DeltaDate.HasValue ||
            staticDimsReportState.DeltaDate.Value < DateTime.UtcNow.Date)
        {
            SendDimRequests(reportItems, queueItem, staticDimReports);
        }
    }

    private void SendDimRequests(List<ApiReportItem> reportItems, Queue queueItem, IEnumerable<APIReport<ReportSettings>> dimReports, List<Mapping> mappingFile = null, bool isAccountReport = false)
    {
        foreach (var report in dimReports)
        {
            var reportParameters = GetParameters(report);

            //if this entity relies on a parent entity to access their info
            if (!string.IsNullOrEmpty(report.ReportSettings.ParentEntity))
            {
                //we retrieve all the children (current item) for all the values retrieved from a previous call to the parent
                // for example AdAccount is a parent of Campaign, the following url retrieves all campaigns for the parent adAccount
                // example: https://adsapi.snapchat.com/v1/adaccounts/2095c88c-9ab7-4b62-bbf5-3c3d27415000/campaigns?&limit=1000
                // this can apply to Country as parent and its children Region, Metro ...

                var parentValues = _parentCache[report.ReportSettings.ParentEntity.ToLower()];

                List<GetReportOptions> options = new();
                parentValues.ForEach(entityID => options.Add(new()
                {
                    Parameters = reportParameters,
                    ApiReport = report,
                    EntityID = entityID,
                    AdAccountID = isAccountReport ? entityID : queueItem.EntityID.ToLower(),
                    HasMappingFile = isAccountReport,
                    QueueItem = queueItem,
                    IsDimension = true
                }));

                MakeParallelCalls(options, reportItems, mappingFile);
            }
            else
            {
                // we query the entity on its own
                // example: https://adsapi.snapchat.com/v1/targeting/geo/country
                var reports = GetReportItem(queueItem, report, true, reportParameters, null, queueItem.EntityID.ToLower(), mappingFile);
                reportItems.AddRange(reports);
            }
        }
    }

    private void GetMeasurementReports(List<ApiReportItem> reportItems, IFileItem queueItem, List<Mapping> mappingFile)
    {
        GetCampaignEntityReports(reportItems, (Queue)queueItem, mappingFile);
        GetCreativesEntityReports(reportItems, (Queue)queueItem, mappingFile);
    }

    private void GetCreativesEntityReports(List<ApiReportItem> reportItems, Queue queueItem, List<Mapping> mappingFile)
    {
        var statsReports = GetStatsReports(EntityType.Creatives);

        if (_attributionReports.TryGetValue(queueItem.EntityID, out List<APIReport<ReportSettings>> value))
        {
            // based on lookup for attribution window, identify any additional conversion reports we need
            statsReports.AddRange(value);
        }

        if (statsReports.Count == 0)
        {
            LogMessage(LogLevel.Warn, "No creatives stats reports found. Returning.");
            return;
        }

        if (!_creativeWhiteListingSettings.AllowAll && !_creativeWhiteListingSettings.WhiteList.Contains(queueItem.EntityID))
        {
            LogMessage(LogLevel.Info, $"Skipping Creative Reports for entity {queueItem.EntityID}");
            return;
        }

        //get accounts and their time zone saved in the lookup (SNAPCHAT_{organizationID})
        var accounts = GetAccountLookup(queueItem, CurrentIntegration.IntegrationID);

        //STEP 1: get creatives data from staged files
        //one creative file for each account
        ///s3: ./stage/snapchatads-aggregate/{orgID}/{importDateTime}/Creatives/{accountID}_Creatives.json
        List<AdAccountCreatives> accountCreativesList = new();

        string[] creativesPaths =
        [
            queueItem.EntityID.ToLower(),
            nameof(EntityType.Creatives).ToLower(),
            CurrentIntegration.IntegrationID.ToString()
        ];

        var creativesUri = RemoteUri.CombineUri(baseRawDestUri, creativesPaths);
        // filter campaign files that start with account id's
        var creativesFiles = remoteAccessClient.WithDirectory(creativesUri).GetFiles()
            .Where(x => accounts.Any(a => x.Name.StartsWith(a.Key)))
            .ToList();

        foreach (var file in creativesFiles)
        {
            string[] filePaths =
            [
                queueItem.EntityID.ToLower(),
                nameof(EntityType.Creatives).ToLower(),
                CurrentIntegration.IntegrationID.ToString(),
                file.Name
            ];
            var creativesRootList = GetReportData<CreativesRoot>(filePaths, false);
            var creativesData = creativesRootList.SelectMany(x => x.Creatives.Select(y => y.Creative)).ToList();
            if (creativesData.Count != 0)
            {
                var matchingAccount = accounts.FirstOrDefault(a => creativesData.Select(c => c.AdAccountId).Contains(a.Key));

                // we keep all campaigns file to handle backfills
                //it is possible for a campaign file to reference an account that was deleted
                if (matchingAccount.Key == null)
                {
                    continue;
                }

                var accountCreativeItem = new AdAccountCreatives()
                {
                    AdAccountID = matchingAccount.Key,
                    Timezone = matchingAccount.Value,
                    Creatives = creativesData
                };
                accountCreativesList.Add(accountCreativeItem);
            }
        }

        //STEP 2: get account time zone for each creative
        var distinctCreatives = accountCreativesList.SelectMany(x => x.Creatives, (x, creative) => new
        {
            x.AdAccountID,
            x.Timezone,
            creative.Id,
            creative.UpdatedAt
        }
        ).GroupBy(x => new
        {
            x.AdAccountID,
            x.Id
        }
        ).Select(g => g.OrderByDescending(x => x.UpdatedAt).FirstOrDefault()).ToList();

        var creatives = distinctCreatives.Select(x => new CreativeTZ()
        {
            AdAccountID = x.AdAccountID,
            Timezone = x.Timezone,
            CreativeID = x.Id
        });

        //STEP 3 (Last step): if any creatives then make stats call using the campaign ID and set the offset using the account-time zone 
        // OR if no campaigns matched criteria then create placeholders (manifest files referencing empty DONE file) to avoid a Redshift-COPY error during processing
        if (!creatives.Any())
        {
            LogMessage(LogLevel.Info, $"No creatives for entity:{queueItem.EntityID.ToLower()} FileDate:{queueItem.FileDate} FileGUID:{queueItem.FileGUID} - Skipping stats download");
            return;
        }

        LogMessage(LogLevel.Debug, $"Getting stats reports for account: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID}; Total creatives: {creatives.Count()}");

        List<GetReportOptions> optionsList = new();

        foreach (var creative in creatives)
        {
            foreach (var report in statsReports)
            {
                var parameters = GetReportParametersWithTimeZone(report, queueItem, creative.Timezone);
                GetReportOptions options = new()
                {
                    Parameters = parameters,
                    ApiReport = report,
                    EntityID = creative.CreativeID,
                    AdAccountID = creative.AdAccountID,
                    HasMappingFile = true,
                    QueueItem = queueItem,
                    IsDimension = false
                };
                optionsList.Add(options);
            }
        }

        MakeParallelCalls(optionsList, reportItems, mappingFile);
    }

    /// <summary>
    /// Gets Stats and Conversions reports with Campaign Entity
    /// </summary>
    private void GetCampaignEntityReports(List<ApiReportItem> reportItems, Queue queueItem, List<Mapping> mappingFile)
    {
        //total of 18 reports (ie for each entity there is one non-segment and five segment reports)
        //entities: 1) campaign, 2) adsquad, and 3) ad
        //segmentation: 1) Country (country, region, dma) and 2) Device (os, make)
        var statsReports = GetStatsReports(EntityType.Campaigns);

        if (_attributionReports.TryGetValue(queueItem.EntityID, out List<APIReport<ReportSettings>> value))
        {
            // based on lookup for attribution window, identify any additional conversion reports we need
            statsReports.AddRange(value);
        }

        //get accounts and their time zone saved in the lookup (SNAPCHAT_{organizationID})
        var accounts = GetAccountLookup(queueItem, CurrentIntegration.IntegrationID);

        //campaign will be the driver of acquiring stats data
        //daily job: get "active" and file-date is in flight
        //backfill: any status and file-date is in flight

        //STEP 1: get campaign data from staged files
        //one campaign file for each account
        ///s3: ./stage/snapchatads-aggregate/{orgID}/{importDateTime}/Campaigns/{accountID}_Campaigns.json
        var accountCampaignsList = new List<AdAccountCampaigns>();

        string[] campaignPaths =
        [
            queueItem.EntityID.ToLower(),
            nameof(EntityType.Campaigns).ToLower(),
            CurrentIntegration.IntegrationID.ToString()
        ];
        var campaignUri = RemoteUri.CombineUri(baseRawDestUri, campaignPaths);
        // filter campaign files that start with account id's
        var campaignFiles = remoteAccessClient.WithDirectory(campaignUri).GetFiles()
            .Where(x => accounts.Any(a => x.Name.StartsWith(a.Key)))
            .ToList();

        foreach (var file in campaignFiles)
        {
            string[] filePaths =
            [
                queueItem.EntityID.ToLower(),
                nameof(EntityType.Campaigns).ToLower(),
                CurrentIntegration.IntegrationID.ToString(),
                file.Name
            ];
            var campaignRootList = GetReportData<CampaignsRoot>(filePaths, false);
            var campaignData = campaignRootList.SelectMany(x => x.Campaigns.Select(y => y.Campaign)).ToList();
            if (campaignData.Count != 0)
            {
                var matchingAccount = accounts.FirstOrDefault(a => campaignData.Select(c => c.AdAccountId).Contains(a.Key));

                // we keep all campaigns file to handle backfills
                //it is possible for a campaign file to reference an account that was deleted
                if (matchingAccount.Key == null) continue;

                var accountCampaignItem = new AdAccountCampaigns()
                {
                    AdAccountID = matchingAccount.Key,
                    Timezone = matchingAccount.Value,
                    Campaigns = campaignData
                };
                accountCampaignsList.Add(accountCampaignItem);
            }
        }

        //STEP 2: get account time zone for each campaign
        var distinctCampaigns = accountCampaignsList.SelectMany(x => x.Campaigns, (x, campaign) => new
        {
            x.AdAccountID,
            x.Timezone,
            campaign.Id,
            campaign.Status,
            campaign.StartTime,
            campaign.EndTime,
            campaign.UpdatedAt
        }
        ).GroupBy(x => new
        {
            x.AdAccountID,
            x.Id
        }
        ).Select(g => g.OrderByDescending(x => x.UpdatedAt).FirstOrDefault()).ToList();

        var campaigns = distinctCampaigns.Select(x => new CampaignTZ()
        {
            AdAccountID = x.AdAccountID,
            Timezone = x.Timezone,
            CampaignID = x.Id,
            Status = x.Status,
            StartTime = x.StartTime,
            EndTime = x.EndTime
        });

        if (!queueItem.IsBackfill)
        {
            campaigns = campaigns.Where(c => campaignDailyStatus.Contains(c.Status));
        }

        //STEP 3: filter to relevant date (ie file date)
        var filteredCampaigns = campaigns.Where(x => ConvertUtcToTimeZone(x.StartDateTime, queueItem.EntityID).AddDays(-nbDayBuffer) <= queueItem.FileDate
                                                     && ConvertUtcToTimeZone(x.EndDateTime, queueItem.EntityID).AddDays(nbDayBuffer) >= queueItem.FileDate);

        //STEP 4 (Last step): if any campaigns then make stats call using the campaign ID and set the offset using the account-time zone 
        // OR if no campaigns matched criteria then create placeholders (manifest files referencing empty DONE file) to avoid a Redshift-COPY error during processing
        if (!filteredCampaigns.Any())
        {
            LogMessage(LogLevel.Info, $"No campaigns for entity:{queueItem.EntityID.ToLower()} FileDate:{queueItem.FileDate} FileGUID:{queueItem.FileGUID} - Skipping stats download");
            return;
        }

        LogMessage(LogLevel.Debug, $"Getting stats reports for account: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID}; Total campaigns: {filteredCampaigns.Count()}");

        List<GetReportOptions> optionsList = new();

        foreach (var campaign in filteredCampaigns)
        {
            foreach (var report in statsReports)
            {
                var parameters = GetReportParametersWithTimeZone(report, queueItem, campaign.Timezone);
                GetReportOptions options = new()
                {
                    Parameters = parameters,
                    ApiReport = report,
                    EntityID = campaign.CampaignID,
                    AdAccountID = campaign.AdAccountID,
                    HasMappingFile = true,
                    QueueItem = queueItem,
                    IsDimension = false
                };
                optionsList.Add(options);
            }
        }

        MakeParallelCalls(optionsList, reportItems, mappingFile);
    }

    private string GetReportParametersWithTimeZone(APIReport<ReportSettings> report, Queue queueItem, string timezone)
    {
        var parameters = GetParameters(report);

        TimeZoneConverter.TZConvert.TryGetTimeZoneInfo(timezone, out var tzi);
        var startTimeOffset = tzi.GetUtcOffset(queueItem.FileDate);

        var endDate = queueItem.FileDate.AddDays(1);
        var endTimeOffset = tzi.GetUtcOffset(endDate);

        //format offset time and escape "+" sign when timezone is ahead of UTC
        bool isNegativeStartTime = startTimeOffset.ToString().StartsWith(Constants.HYPHEN);
        var startTimeOffsetString = isNegativeStartTime ? $"{startTimeOffset}" : $"%2B{startTimeOffset}";
        bool isNegativeEndTime = endTimeOffset.ToString().StartsWith(Constants.HYPHEN);
        var endTimeOffsetString = isNegativeEndTime ? $"{endTimeOffset}" : $"%2B{endTimeOffset}";

        parameters += $"&start_time={queueItem.FileDate:yyyy-MM-dd}T00:00:00.000{startTimeOffsetString}&end_time={endDate:yyyy-MM-dd}T00:00:00.000{endTimeOffsetString}";

        return parameters;
    }

    private List<APIReport<ReportSettings>> GetStatsReports(EntityType entityType)
    {
        return _reports.Where(r => MetricReports.Any(mr => r.ReportSettings.ReportType.Equals(mr, StringComparison.InvariantCultureIgnoreCase)) && r.IsActive && r.ReportSettings.Entity == entityType.ToString()).ToList();
    }

    public DateTime ConvertUtcToTimeZone(DateTime date, string entityID)
    {
        string timeZone = APIEntityRepository.GetAPIEntityTimeZone(entityID, APIEntities, CurrentIntegration);

        var timeZoneInfo = TZConvert.GetTimeZoneInfo(timeZone);

        return TimeZoneInfo.ConvertTimeFromUtc(date, timeZoneInfo).Date;
    }

    private Dictionary<string, string> GetAdAccounts(List<ApiReportItem> reportItems, Queue queueItem)
    {
        var dimensionReports = _reports.Where(r => r.ReportSettings.ReportType.Equals("dimension") && r.IsActive);
        var accountReport = dimensionReports.First(x => x.ReportSettings.Entity.Equals("AdAccounts"));
        var accountParameters = GetParameters(accountReport);
        var accountReports = GetReportItem(queueItem, accountReport, true, accountParameters, queueItem.EntityID.ToLower(), queueItem.EntityID.ToLower());
        reportItems.AddRange(accountReports);

        var adAccountRootList = new List<AdAccountsRoot>();

        foreach (var accountsReportItem in accountReports)
        {
            string[] paths =
            [
                queueItem.EntityID.ToLower(),
                GetDatedPartition(queueItem.FileDate),
                accountsReportItem.ReportName.ToLower(),
                accountsReportItem.FileName
            ];
            adAccountRootList.AddRange(GetReportData<AdAccountsRoot>(paths, false));
        }

        return SaveAccountLookup(queueItem, adAccountRootList, CurrentIntegration.IntegrationID);
    }

    private static string AccountLookupKey(Queue queue, Int64 integrationID) => string.Format(null, _snapchat_account_prefix, queue.EntityID.ToLower(), integrationID);

    private static Dictionary<string, string> GetAccountLookup(Queue queue, Int64 integrationID)
    {
        return LookupService.GetAndDeserializeLookupValueWithDefault<Dictionary<string, string>>(AccountLookupKey(queue, integrationID), []);
    }

    private static Dictionary<string, string> SaveAccountLookup(Queue queue, List<AdAccountsRoot> adAccountRootList, Int64 integrationID)
    {
        var adAccounts = adAccountRootList.SelectMany(x => x.Adaccounts.Select(y => y.Adaccount));

        //create dictionary of accountID and timezone
        var accountDict = new Dictionary<string, string>();

        foreach (var account in adAccounts)
        {
            accountDict.Add(account.Id, account.Timezone);
        }

        string accountsJson = JsonConvert.SerializeObject(accountDict);

        var orgLookup = SetupService.GetById<Lookup>(AccountLookupKey(queue, integrationID));

        if (orgLookup != null)
        {
            orgLookup.Value = accountsJson;
            orgLookup.LastUpdated = DateTime.Now;
            SetupService.Update(orgLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(AccountLookupKey(queue, integrationID), accountsJson);
        }
        return accountDict;
    }

    private List<ApiReportItem> GetReportItem(Queue queueItem, APIReport<ReportSettings> report, bool isDimension, string parameters, string entityID, string accountID, List<Mapping> mappingFile = null)
    {
        var reportRequest = new ApiReportRequest()
        {
            AccountID = accountID,
            OrganizationID = queueItem.EntityID.ToLower(),
            IsDimension = isDimension,
            IsBackfill = queueItem.IsBackfill,
            EntityName = report.ReportSettings.Entity,
            ParentName = report.ReportSettings.ParentEntity,
            URLPath = report.ReportSettings.URLPath,
            EntityId = entityID,
            MethodType = System.Net.Http.HttpMethod.Get,
            Parameters = parameters,
            Endpoint = CurrentIntegration.EndpointURI
        };

        var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
        {
            Counter = 0,
            MaxRetry = _maxRetry
        };

        var retry = new CancellableRetry(this.JED.JobGUID.ToString(), apiCallsBackOffStrategy, _runTime, maxRuntime);

        var reports = snapchatProvider.GetSnapchatReport(queueItem, report, reportRequest, _parentCache, retry
            , (stream, fileName) => UploadFileToS3(queueItem, fileName, report.APIReportName, stream)
            , mappingFile);

        return reports;
    }

    private void SaveState()
    {
        var lookupKey = $"{Constants.SNAPCHAT_STATE}_{CurrentIntegration.IntegrationID}";
        var dbState = SetupService.GetById<Lookup>(lookupKey);

        if (dbState != null)
        {
            var snapchatStateLookup = new Lookup
            {
                Name = lookupKey,
                Value = JsonConvert.SerializeObject(snapchatReportState)
            };
            SetupService.Update(snapchatStateLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(lookupKey, JsonConvert.SerializeObject(snapchatReportState));
        }
    }

    private void FinalizeQueue(List<ApiReportItem> reportItems, IFileItem queueItem, DateTime importDateTime, List<Mapping> mappingFile)
    {
        LogMessage(LogLevel.Info, $"All reports complete for Entity:{queueItem.EntityID}; FileDate:{queueItem.FileDate}; fileguid:{queueItem.FileGUID}");

        CreateMappingFile((Queue)queueItem, mappingFile);

        // Deleting staged manifest files by fileguid before creating new ones
        // It is important to delete them to prevent any issue when queues are set to Import Pending after Processing failed
        // otherwise outdated dim manifest could still exist and cause issue (no matching file in raw)
        var dirPath = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
        DeleteStageFiles(dirPath, queueItem.FileGUID, queueItem.FileGUID.ToString());
        List<FileCollectionItem> fileItems = new();
        if (!this.allCreatedFiles.IsEmpty)
        {
            fileItems.AddRange(this.allCreatedFiles);
        }
        var fileCollection = ETLProvider.CreateManifestFiles((Queue)queueItem, fileItems, baseStageDestUri, GetDatedPartition);

        LogMessage(LogLevel.Debug, $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}");

        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(fileCollection);
        queueItem.FileSize += fileCollection.Sum(f => f.FileSize);
        queueItem.DeliveryFileDate = UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, importDateTime);
        queueItem.Status = nameof(Constants.JobStatus.Complete);
        queueItem.StatusId = (int)Constants.JobStatus.Complete;

        JobService.Update<Queue>((Queue)queueItem);

        if (reportItems.Any(r => r.ReportType == "dimension"))
        {
            //update snapchat state after updating queue record to ensure that the file collection is preserved and the data will be processed
            staticDimsReportState.DeltaDate = DateTime.UtcNow.Date;
        }

        currentReportState.DeltaDate = importDateTime.Date;

        SaveState();
    }
    private void CreateMappingFile(Queue queueItem, List<Mapping> mappingFile)
    {
        int i = 0;
        do
        {
            i++;
            int firstRecordPosition = (i - 1) * mappingRecordsPerFile;
            var records = mappingFile.Skip(firstRecordPosition).Take(mappingRecordsPerFile);

            var fileName = ApiReportItem.GenerateFileName(queueItem, "mappingFile", fileCounter: i - 1);

            var remotePath = new string[]
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName
            };

            Uri remoteUri = RemoteUri.CombineUri(baseRawDestUri, remotePath);
            S3File s3File = new(remoteUri, GreenhouseS3Creds);

            using (var stream = new MemoryStream())
            {
                using var writer = new StreamWriter(stream);
                writer.Write("{\"requests\":" + JsonConvert.SerializeObject(records) + "}");
                writer.Flush();
                stream.Position = 0;
                s3File.Put(stream);
            }

            var fileItem = new FileCollectionItem { FileSize = s3File.Length, SourceFileName = "MappingFile", FilePath = s3File.ToString().TrimStart('/') };
            this.allCreatedFiles.Add(fileItem);
        } while (i * mappingRecordsPerFile <= mappingFile.Count);
    }

    private List<T> GetReportData<T>(string[] paths, bool isLocal = false, Newtonsoft.Json.JsonSerializerSettings jsonSettings = null) where T : new()
    {
        var statsData = new List<T>();
        jsonSettings ??= new Newtonsoft.Json.JsonSerializerSettings()
        {
            MissingMemberHandling = MissingMemberHandling.Ignore,
            NullValueHandling = NullValueHandling.Ignore
        };

        var sourceUri = isLocal ? this.baseLocalRawUri : this.baseRawDestUri;
        var filePath = RemoteUri.CombineUri(sourceUri, paths);

        using (var sourceStream =
            isLocal ? File.OpenRead(filePath.AbsolutePath) : remoteAccessClient.WithFile(filePath).Get())
        {
            using var txtReader = new StreamReader(sourceStream);
            //the json raw file is a list of api response objects
            //we wrap the raw data with the AllData class so that we can deserialize the json as an array
            var rootStatsJob =
                JsonConvert.DeserializeObject<AllData<T>>($"{{'allData':[{txtReader.ReadToEnd()}]}}",
                    jsonSettings);
            if (rootStatsJob.allData.Count != 0) statsData.AddRange(rootStatsJob.allData);
        }

        return statsData;
    }

    private enum EntityType
    {
        Campaigns,
        Creatives
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
            //dispose of stuff here
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }

    [GeneratedRegex("[^a-zA-Z0-9_]+", RegexOptions.Compiled)]
    private static partial System.Text.RegularExpressions.Regex CleanReportNamesRegex();

    private static void ThrottleCalls<T>(List<T> source, int nbItems, int timeWindowMilliseconds, Action<string> logInfo, Action<IEnumerable<T>> action)
    {
        var importStopWatch = System.Diagnostics.Stopwatch.StartNew();
        var subLists = source.Chunk(nbItems);
        foreach (var list in subLists)
        {
            action(list);

            // have we made _maxAPIRequestPer60s calls in less than a minute? if so we wait
            long remainingTime = timeWindowMilliseconds - importStopWatch.ElapsedMilliseconds;
            if (remainingTime > 0)
            {
                logInfo($"Queries per minute quota reached - Pausing for {remainingTime} ms");
                Task.Delay((int)remainingTime).Wait();
            }
            importStopWatch = System.Diagnostics.Stopwatch.StartNew();
        }
    }

    private void MakeParallelCalls(List<GetReportOptions> options, List<ApiReportItem> reportItems, List<Mapping> mappingFile)
    {
        ConcurrentQueue<Exception> concurrentExceptions = new();
        ConcurrentBag<ApiReportItem> concurrentReportItems = new();
        ConcurrentBag<Mapping> concurrentMappingFiles = new();
        ThrottleCalls(options, _maxRequestsPerWindow, _apiRequestTimeWindow, msg => LogMessage(LogLevel.Info, msg), (optionSubList) =>
        {
            Parallel.ForEach(optionSubList, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (option, state) =>
            {
                try
                {
                    List<Mapping> mappingFiles = new();
                    var reports = option.HasMappingFile
                        ? GetReportItem(option.QueueItem, option.ApiReport, option.IsDimension, option.Parameters, option.EntityID, option.AdAccountID, mappingFiles)
                        : GetReportItem(option.QueueItem, option.ApiReport, option.IsDimension, option.Parameters, option.EntityID, option.AdAccountID);
                    if (reports.Count > 0)
                    {
                        reports.ForEach(x => concurrentReportItems.Add(x));
                    }

                    if (mappingFiles.Count > 0)
                    {
                        mappingFiles.ForEach(x => concurrentMappingFiles.Add(x));
                    }
                }
                catch (Exception ex)
                {
                    LogException(LogLevel.Error, $"{option.QueueItem.FileGUID}-Download failed - report:{option.ApiReport.APIReportName}-entity:{option.EntityID}|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                    concurrentExceptions.Enqueue(ex);
                    state.Stop();
                }
            });
        });

        if (!concurrentExceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(concurrentExceptions.First()).Throw();
        }

        if (!concurrentReportItems.IsEmpty)
        {
            reportItems.AddRange(concurrentReportItems);
        }

        if (!concurrentMappingFiles.IsEmpty)
        {
            mappingFile.AddRange(concurrentMappingFiles);
        }
    }

    private void UploadFileToS3(Queue queueItem, string fileName, string reportName, Stream stream)
    {
        string[] paths =
        [
            queueItem.EntityID.ToLower(),
            GetDatedPartition(queueItem.FileDate),
            reportName.ToLower(),
            fileName
        ];

        stream.Seek(0, SeekOrigin.Begin);

        S3File rawFile = new(RemoteUri.CombineUri(baseRawDestUri, paths), GreenhouseS3Creds);
        StreamFile incomingFile = new(stream, GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);

        var fileItem = new FileCollectionItem { FileSize = rawFile.Length, SourceFileName = reportName.ToLower(), FilePath = rawFile.ToString().TrimStart('/') };
        this.allCreatedFiles.Add(fileItem);

        CacheCampaignAndCreativeEntities(queueItem, fileName, reportName, rawFile);
    }

    private void CacheCampaignAndCreativeEntities(Queue queueItem, string fileName, string reportName, S3File sourceFile)
    {
        if (reportName.Equals("campaigns", StringComparison.CurrentCultureIgnoreCase) || reportName.Equals("creatives", StringComparison.CurrentCultureIgnoreCase))
        {
            string[] paths =
            [
                queueItem.EntityID.ToLower(),
                reportName.ToLower(),
                CurrentIntegration.IntegrationID.ToString(),
                fileName.Replace($"{queueItem.FileGUID.ToString().ToLower()}_", "")
            ];

            S3File cachedFile = new(RemoteUri.CombineUri(baseRawDestUri, paths), GreenhouseS3Creds);
            sourceFile.CopyTo(cachedFile);
        }
    }

    private void LogMessage(LogLevel logLevel, string message)
    {
        logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(message)));
    }

    private void LogException(LogLevel logLevel, string message, Exception exc = null)
    {
        logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(message), exc));
    }
}

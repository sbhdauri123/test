using Greenhouse.Common;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using TimeZoneConverter;

namespace Greenhouse.Jobs.Framework;

[Export("GenericInitializeAggregateJob", typeof(IDragoJob))]
public class AggregateInitializeJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private const string JOB_CACHE_REPORT_PREFIX = "AGGREGATE_INIT_JOB_CACHE";

    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private static readonly QueueRepository _queueRepository = new QueueRepository();
    private List<IFileItem> _currentlyQueued;
    private IEnumerable<APIEntity> _APIEntities;
    private IEnumerable<Integration> _integrations;
    private AggregateInitializeSettings _aggregateInitializeSettings;
    private TrueUpConfiguration _trueUpConfiguration;
    private Lookup _backfillDetailsLookup;
    private Dictionary<string, int> _bucketSizes;
    private Dictionary<string, BackfillDetail> _backfillDetails;
    private Action<LogLevel, string> _log;
    private string _backfillDetailsName;
    private int _defaultBucketSize;
    private enum BackFillType
    {
        DailyNonBackFill, // Regular scheduled Job daily scheduled based on Source.AggregateInitializeSettings.RegularAggregateDays
        DailyBackFill, // Regular scheduled Job daily scheduled based on Source.AggregateInitializeSettings.BackFillAggregateDays
        ManualBackFill, // One time scheduled BackFill job
        ManualDimensionBackFill // One time scheduled BackFill job retrieving Dimensions only
    }
    private bool _hasWarning;
    private List<AggregateInitCacheEntry> _aggInitCache;
    private string _cacheReportLookupName;

    public void PreExecute()
    {
        _log = (logLevel, msg) =>
        {
            if (logLevel == LogLevel.Warn)
            {
                _hasWarning = true;
            }
            _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
        };

        // AggregateInitJobCache stores list of entities that have queues created for the daily job to avoid having to check database
        _cacheReportLookupName = $"{JOB_CACHE_REPORT_PREFIX}_{CurrentSource.SourceID}";
        _aggInitCache = LookupService.GetAndDeserializeLookupValueWithDefault(_cacheReportLookupName, new List<AggregateInitCacheEntry>());
    }

    private bool InitializeDate()
    {
        if (!string.IsNullOrEmpty(CurrentSource.AggregateInitializeSettings))
        {
            _aggregateInitializeSettings =
                ETLProvider.DeserializeType<AggregateInitializeSettings>(CurrentSource.AggregateInitializeSettings);
        }

        SourceJobStep _nextStep = JED.ExecutionPath.GotoNextStep() ?? throw new ArgumentNullException(string.Format("No Next Step. Initialize always expects a next step to be defined in the ExecutionPath for it to execute."));
        _log(LogLevel.Debug, string.Format("{1} - Next Step is: {0} ", _nextStep, this.JED.JobGUID));

        _currentlyQueued = JobService.GetAllQueueItemsBySource(CurrentSource.SourceID).ToList();
        _APIEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID);
        _log(LogLevel.Info, $"{this.JED.JobGUID} - Initialized - Found CurrentlyQueued: {_currentlyQueued.Count} Entities: {_APIEntities.Count()}");

        var trueUpConfigurationValue = JobService.GetById<Lookup>(Constants.TRUEUPCONFIGURATION).Value;
        _trueUpConfiguration = ETLProvider.DeserializeType<TrueUpConfiguration>(trueUpConfigurationValue);

        _bucketSizes = new Dictionary<string, int>();
        _backfillDetailsLookup = new Lookup();
        _backfillDetails = new Dictionary<string, BackfillDetail>();

        _backfillDetailsName = CurrentSource.SourceName + Constants.AGGREGATE_BACKFILL_DETAILS_SUFFIX;

        if (_aggregateInitializeSettings.UseBFDateBucket)
        {
            //retrieve the bucket size for each apientity
            var bucketsJson = JobService.GetById<Lookup>(CurrentSource.SourceName + Constants.AGGREGATE_BUCKET_SIZE_SUFFIX)?.Value;

            if (bucketsJson != null)
            {
                _bucketSizes = ETLProvider.DeserializeType<Dictionary<string, int>>(bucketsJson);
            }

            _backfillDetailsLookup = JobService.GetById<Lookup>(_backfillDetailsName) ?? new Lookup
            {
                Name = _backfillDetailsName,
                IsEditable = false
            };

            if (_backfillDetailsLookup.Value != null)
            {
                _backfillDetails = ETLProvider.DeserializeType<Dictionary<string, BackfillDetail>>(_backfillDetailsLookup.Value);
            }
        }

        _defaultBucketSize = _bucketSizes.TryGetValue("default", out int value) ? value : 7;

        // returns integrations active or inactive 
        _integrations = SetupService.GetAll<Integration>().Where(i => i.SourceID == this.SourceId);

        return true;
    }

    public void Execute()
    {
        if (!InitializeDate())
        {
            return;
        }

        if (_APIEntities?.Any() == true)
        {
            var queuesToInsert = new List<Queue>();

            if (JED.ScheduleCalendar.Interval != ScheduleCalendar.IntervalType.Backfill)
            {
                bool alreadyRunToday = CreateQueueForRegularJob(out queuesToInsert);

                if (!alreadyRunToday)
                {
                    // get list of queue for the current source - to be used to clean
                    _currentlyQueued = JobService.GetAllQueueItemsBySource(CurrentSource.SourceID).ToList();

                    // true up job should be executed once a day to avoid creating duplicates
                    CreateQueueForTrueUp();
                }
            }
            else
            {
                CreateQueueForManualBackfill(out queuesToInsert);
            }

            if (_aggregateInitializeSettings.UseBFDateBucket)
            {
                // cleaning up Lookup  value BackfillDetails from any queue that has been processed
                // this code should be in the processing job. But only AggregateInitializeJob
                // should be editing the lookup value BackfillDetails to avoid data being
                // overwritten in case of Processing jobs running at the same time as AggregateInitializeJob
                var currentFileGUID = _currentlyQueued.Select(q => q.FileGUID.ToString());
                var fileGuidsToInsert = queuesToInsert.Select(q => q.FileGUID.ToString());
                var processedFileGUID = _backfillDetails.Where(b => !fileGuidsToInsert.Contains(b.Key) && !currentFileGUID.Contains(b.Key))
                    .Select(b => b.Key).ToList();
                if (processedFileGUID.Count != 0)
                {
                    foreach (string fileGUID in processedFileGUID)
                    {
                        _backfillDetails.Remove(fileGUID);
                    }
                }

                // saving _backfillDetails in Lookup table
                _backfillDetailsLookup.Value = JsonConvert.SerializeObject(_backfillDetails);
                LookupRepository.AddOrUpdateLookup(_backfillDetailsLookup);
            }

            _queueRepository.BulkInsert(queuesToInsert);
            LookupService.SaveJsonObject(_cacheReportLookupName, _aggInitCache);
        }
        else
        {
            _log(LogLevel.Info, $"Aggregate Initialize - No APIEntity found for SourceID= {CurrentSource.SourceID}. Skipping Initialize Job");
        }

        // the import job is scheduled by the Initialize Job for regular jobs only.
        // Because a backfill job could be scheduled at the same time as a regular job. As a result 2
        // import jobs could be running simultaneously causing issues. 
        if (JED.ScheduleCalendar.Interval != ScheduleCalendar.IntervalType.Backfill)
        {
            // The Import step is scheduled only by the Initialize job
            ScheduleImportJob();
        }

        if (_hasWarning)
        {
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = "Check Splunk for Important Warning messages.";
        }
    }

    private bool CreateQueueForRegularJob(out List<Queue> queuesToInsert)
    {
        queuesToInsert = new List<Queue>();

        if (!(_APIEntities?.Count() > 0))
        {
            _log(LogLevel.Info, $"Regular Schedule Job - No APIEntity found for SourceID= {CurrentSource.SourceID} ");
            return false;
        }

        // getting the daily offset dates for this source for Today
        // if offsets are -1, -2, -8
        // for 10/14 UTC, the UTC dates will be 10/13, 10/12 and 10/05
        // we are using UTC dates as each API Entity has a Timezone, the UTC dates will be converted to that timezone
        // so base on the time of the day we will get different dates 
        var queueInfoList = GetQueueInfoList();
        if (queueInfoList.Count == 0)
        {
            return false;
        }

        bool hasDailyJobRun = false;
        // First we are generating the list of APIEntity/Date for this source
        foreach (var apiEntity in _APIEntities)
        {
            Integration integration = GetValidIntegration(apiEntity);
            if (integration == null)
            {
                continue;
            }
            CurrentIntegration = integration;

            // apply the entity timezone to the offset dates
            // then only keep the date part as it will used as FileLog.FileDate
            IEnumerable<QueueInfo> zonedQueueInfo = ApplyTimeZoneToUtcDates(apiEntity.APIEntityCode, queueInfoList);
            var fileDates = zonedQueueInfo.Select(z => z.DateTime);

            // Performance boost: if queues have been created for an entity, that apientity can be ignored until new filedates are requested
            // the DB call JobServ.GetActiveAPIEntityFileLogs can take 1 to 2 seconds per entity each time the job runs
            // the caching mechanism is to prevent those calls to be made every time
            if (_aggInitCache.Any(c => c.APIEntityCode == apiEntity.APIEntityCode &&
                                       c.IntegrationID == apiEntity.IntegrationID &&
                                       c.MaxFileDate == fileDates.Max()))
            {
                hasDailyJobRun = true;
                _log(LogLevel.Info, $"{this.JED.JobGUID} - APIEntityCode={apiEntity.APIEntityCode} IntegrationID={apiEntity.IntegrationID} skipped because found in cache report having queues already created today");
                continue;
            }
            else
            {
                // there is no match for max filedate, we clean up any old entry for that APIEntityCode + Integration
                _aggInitCache.RemoveAll(c => c.APIEntityCode == apiEntity.APIEntityCode &&
                                       c.IntegrationID == apiEntity.IntegrationID);

                // at this point either the queues are going to be created, or not if they already exist in the queue
                // either way we should not check that apientity until it has a new set of filedate (with a max filedate higher
                // than the one saved in _aggInitCache)
                _aggInitCache.Add(new AggregateInitCacheEntry
                {
                    APIEntityCode = apiEntity.APIEntityCode,
                    IntegrationID = apiEntity.IntegrationID,
                    MaxFileDate = fileDates.Max()
                });
            }

            // if all those dates exists in filelog for that apientity, the daily job ran already today
            // and nothing else need to be done with that apiEntity
            DateTime startDate = apiEntity.StartDate?.Date ?? CurrentIntegration.FileStartDate.Date;
            //if minimum of offset filedates is older then use as start date
            var minFileDate = fileDates.Min(f => f);
            if (minFileDate.Date < startDate.Date)
            {
                startDate = minFileDate;
            }
            var fileLog = JobService.GetActiveAPIEntityFileLogs(CurrentSource.SourceID, apiEntity.IntegrationID, apiEntity.APIEntityCode, startDate).ToList();

            _log(LogLevel.Info, $"{this.JED.JobGUID} - APIEntityCode={apiEntity.APIEntityCode} StartDate={startDate} Found ProcessedFiles:{fileLog.Count}");
            IEnumerable<IFileItem> matchingFileLogs = fileLog.Where(f => fileDates.Contains(f.FileDate));

            var matchingFileDates = matchingFileLogs.Select(f => f.FileDate).Distinct();

            if (fileDates.All(f => matchingFileDates.Contains(f)))
            {
                hasDailyJobRun = true;

                string fileDatesString = string.Join(", ", fileDates.Select(f => f.ToString("MM/dd/yyyy")));
                _log(LogLevel.Info, $"APIEntity with APIEntityCode={apiEntity.APIEntityCode} skipped, all FileLogs with FileDate ({fileDatesString}) already exists.");
                continue;
            }

            foreach (QueueInfo info in zonedQueueInfo)
            {
                // if a queue for that entity and date, has not been imported yet, no need to create a new queue (Backfill or not)
                IEnumerable<IFileItem> fileLogImportNotFinished =
                    _currentlyQueued.Where(q => q.EntityID == apiEntity.APIEntityCode
                            && q.FileDate == info.DateTime
                            && string.Equals(q.Step, Constants.JobStep.Import.ToString(), StringComparison.InvariantCultureIgnoreCase)
                            && !string.Equals(q.Status, Constants.JobStatus.Complete.ToString(), StringComparison.InvariantCultureIgnoreCase));

                if (fileLogImportNotFinished.Any())
                {
                    _log(LogLevel.Info,
                        $"Not creating a new queue for APIEntityCode={apiEntity.APIEntityCode} and FileDate={info.DateTime:MM/dd/yyyy}, another queue (ID={fileLogImportNotFinished.First().ID}) Step='{fileLogImportNotFinished.First().Step}' Status='{fileLogImportNotFinished.First().Status}'");
                    continue;
                }

                var backFillType = info.IsBackFill ? BackFillType.DailyBackFill : BackFillType.DailyNonBackFill;

                Queue queue = CreateQueueItems(info.DateTime, apiEntity, backFillType, "Daily Offset");
                queuesToInsert.Add(queue);
            }

            //if any reports are missing in FileLog between the startDate and now (almost, see comment next line), we create Backfill queues
            // the end range (max) will be the min fileDate - for example offset -8 in the case of (-1,-2, -8)
            // as the data after that is not settled yet
            var allFileDate = ETLProvider.GenerateDatesBetween(startDate.Date, minFileDate);

            var alreadyCreated = fileLog.Select(s => s.FileDate);
            var missingDates = allFileDate // all the dates data should be imported and processed for that apientity
                .Except(fileDates) // the dates a queue will be created by this job - this should only remove the min daily date
                .Except(alreadyCreated)
                .ToList(); // the dates a filelog is created backfill or not (so not missing)

            string missingDatesString = string.Join(", ", missingDates.Select(d => d.ToString("MM/dd/yyyy")));
            _log(LogLevel.Info, $"Found missing reports for {missingDates.Count} unique dates. Creating backfill queues for APIEntityCode={apiEntity.APIEntityCode} and FileDate={missingDatesString}");

            if (_aggregateInitializeSettings.UseBFDateBucket)
            {
                _log(LogLevel.Info, "UseBFDateBucket = true - breaking down the dates to group by date buckets");
                int bucketSize = _bucketSizes.TryGetValue(apiEntity.APIEntityCode, out int value) ? value : _defaultBucketSize;
                UtilsDate.BreakDownDates(missingDates, out List<List<DateTime>> consecutiveDates,
                    out List<DateTime> singleDates, bucketSize);

                foreach (var dates in consecutiveDates)
                {
                    var bucketQueues = CreateBucketQueues(dates.Min(), dates.Max(), apiEntity, BackFillType.DailyBackFill);
                    queuesToInsert.AddRange(bucketQueues);
                    _log(LogLevel.Info, $"{bucketQueues.Count} date bucket queues created");
                }

                var singleQueues = CreateQueueItems(singleDates, apiEntity, BackFillType.DailyBackFill, "Missing date");
                queuesToInsert.AddRange(singleQueues);
                _log(LogLevel.Info, $"{singleQueues.Count} single date queues created");
            }
            else
            {
                var queues = CreateQueueItems(missingDates, apiEntity, BackFillType.DailyBackFill, "Missing date");
                queuesToInsert.AddRange(queues);
                _log(LogLevel.Info,
                    $"UseBFDateBucket = false - Queued {missingDates.Count} backfill reports for processing for Entity: {apiEntity.APIEntityCode}");
            }
        }

        return hasDailyJobRun;
    }

    private IEnumerable<QueueInfo> ApplyTimeZoneToUtcDates(string entityID, IEnumerable<QueueInfo> queueInfoList)
    {
        var timeZone = APIEntityRepository.GetAPIEntityTimeZone(entityID, _APIEntities, CurrentIntegration);
        var timeZoneInfo = TZConvert.GetTimeZoneInfo(timeZone);

        return queueInfoList.Select(d => new QueueInfo(TimeZoneInfo.ConvertTimeFromUtc(d.DateTime, timeZoneInfo).Date, d.IsBackFill));
    }

    private DateTime ApplyTimeZoneToUtcDate(APIEntity entity, DateTime date)
    {
        CurrentIntegration = GetValidIntegration(entity);
        var timeZone = APIEntityRepository.GetAPIEntityTimeZone(entity.APIEntityCode, _APIEntities, CurrentIntegration);
        var timeZoneInfo = TZConvert.GetTimeZoneInfo(timeZone);

        return TimeZoneInfo.ConvertTimeFromUtc(date, timeZoneInfo);
    }

    private void CreateQueueForManualBackfill(out List<Queue> queuesToInsert)
    {
        queuesToInsert = new List<Queue>();

        bool isBackFillDimOnly = Convert.ToBoolean(JED.JobProperties[Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DIM_ONLY]);

        DateTime startDate;
        DateTime endDate;
        BackFillType backFillType;

        if (isBackFillDimOnly)
        {
            var today = DateTime.UtcNow;
            startDate = today;
            endDate = today;
            backFillType = BackFillType.ManualDimensionBackFill;
        }
        else
        {
            startDate = DateTime.ParseExact(JED.JobProperties[Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DATE_FROM].ToString(), "yyyy/MM/dd", CultureInfo.InvariantCulture);
            endDate = DateTime.ParseExact(JED.JobProperties[Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DATE_TO].ToString(), "yyyy/MM/dd", CultureInfo.InvariantCulture);
            backFillType = BackFillType.ManualBackFill;
        }

        List<string> selectedApiEntities = JsonConvert.DeserializeObject<List<string>>(JED.JobProperties[Greenhouse.Common.Constants.AGGREGATE_API_ENTITYIDS].ToString());

        if (!(selectedApiEntities?.Count > 0))
        {
            var message = "Error: there are no Entities included in the backfill job!";
            _log(LogLevel.Error, message);
            throw new AggregateInitializeException(message);
        }

        var apiEntities = _APIEntities.Where(x => selectedApiEntities.Contains(x.APIEntityID.ToString()));

        // use date buckets if source configured to do so
        // unless the backFill is for dimensions only (dimensions are not retrieved by date)
        if (_aggregateInitializeSettings.UseBFDateBucket && !isBackFillDimOnly)
        {
            _log(LogLevel.Info, "aggregateInitializeSettings.UseBFDateBucket = true - Creating bucket queues");

            foreach (var entity in apiEntities)
            {
                Integration integration = GetValidIntegration(entity);
                if (integration == null)
                {
                    continue;
                }
                CurrentIntegration = integration;

                var queues = CreateBucketQueues(startDate, endDate, entity, backFillType);
                queuesToInsert.AddRange(queues);
            }
        }
        else
        {
            _log(LogLevel.Info, "aggregateInitializeSettings.UseBFDateBucket = false or backfillDimOnly option selected - Creating consecutive queues");
            foreach (var entity in apiEntities)
            {
                CurrentIntegration = GetValidIntegration(entity);

                if (CurrentIntegration == null)
                {
                    continue;
                }

                var dates = new List<DateTime>();

                for (DateTime date = startDate; date <= endDate; date = date.AddDays(1))
                    dates.Add(date);
                var queues = CreateQueueItems(dates, entity, backFillType, "Backfill no bucket");
                queuesToInsert.AddRange(queues);

                _log(LogLevel.Info, $"Backfill Queued {dates.Count} daily reports for processing for Entity: {entity.APIEntityCode}");
            }
        }

        if (!(apiEntities?.Count() > 0))
        {
            _log(LogLevel.Info, $"BackFill - No APIEntity found for SourceID= {CurrentSource.SourceID} ");
        }
    }

    private Integration GetValidIntegration(APIEntity entity)
    {
        var integration = _integrations.FirstOrDefault(i => i.IntegrationID == entity.IntegrationID);

        if (integration == null || !integration.IsActive)
        {
            _log(LogLevel.Warn,
                $"BackFill - Integration {integration.IntegrationName} ({integration.IntegrationID}) for APIEntityCode {entity.APIEntityCode}) " + (integration == null ? "does not exists" : "is inactive"));
            return null;
        }

        if (integration.ParentIntegrationID != null)
        {
            _log(LogLevel.Warn,
                $"Job - Integration {integration.IntegrationName} ({integration.IntegrationID}) for APIEntityCode {entity.APIEntityCode} has a ParentIntegrationID not NULL ({integration.ParentIntegrationID})");
            return null;
        }

        return integration;
    }

    private List<Queue> CreateBucketQueues(DateTime startDate, DateTime endDate, APIEntity entity, BackFillType backFillType)
    {
        var queuesToInsert = new List<Queue>();
        int bucketSize = _bucketSizes.TryGetValue(entity.APIEntityCode, out int value) ? value : _defaultBucketSize;

        var dateBuckets = UtilsDate.CreateDateBuckets(startDate, endDate, bucketSize);
        foreach (var bucket in dateBuckets)
        {
            var queue = CreateQueueItems(bucket.endDate, entity, backFillType, "Backfill with bucket");

            _log(LogLevel.Info,
                $"Backfill Bucket Queue with FileGuid= {queue.FileGUID} created for start date={bucket.startDate} and end date={bucket.endDate}");

            string fileGuid = queue.FileGUID.ToString();

            _backfillDetails.Add(fileGuid, new BackfillDetail
            {
                StartDate = bucket.startDate,
                EndDate = bucket.endDate,
                CreatedDate = DateTime.Now
            });

            queuesToInsert.Add(queue);
        }

        return queuesToInsert;
    }

    private void CreateQueueForTrueUp()
    {
        if (!(_aggregateInitializeSettings?.TrueUpDetails?.Count > 0))
        {
            _log(LogLevel.Info, $"No TrueUp Job details found for SourceID={CurrentSource.SourceID}");
            return;
        }

        if (!(_APIEntities?.Count() > 0))
        {
            _log(LogLevel.Info, $"TrueUp - No APIEntity found for SourceID={CurrentSource.SourceID}");
            return;
        }

        var dateExpression = GenerateDateExpression();
        var alreadyCreatedInFileLog = false;
        AggregateInitializeSettings.TrueUp matchingTrueUp = null;

        foreach (var trueUpDetails in _aggregateInitializeSettings?.TrueUpDetails.OrderByDescending(t => t.Priority))
        {
            foreach (APIEntity entity in _APIEntities)
            {
                if (trueUpDetails.QueueIntegrity == AggregateInitializeSettings.QueueIntegrityEnum.OnlyLatest)
                {
                    var triggerDay = GetTriggerDate(trueUpDetails.TriggerDayRegex, entity);
                    alreadyCreatedInFileLog = JobService.GetActiveAPIEntityFileLogs(entity.SourceID, entity.IntegrationID, entity.APIEntityCode, triggerDay).Any();
                }
                else if (trueUpDetails.QueueIntegrity == AggregateInitializeSettings.QueueIntegrityEnum.AllCreated)
                {
                    throw new NotImplementedException();
                }

                if (!alreadyCreatedInFileLog)
                {
                    break;
                }
            }

            if (!alreadyCreatedInFileLog || Regex.IsMatch(dateExpression, trueUpDetails.TriggerDayRegex, RegexOptions.IgnoreCase))
            {
                matchingTrueUp = trueUpDetails;
                break;
            }
        }

        if (matchingTrueUp == null)
        {
            _log(LogLevel.Info, $"No matching TrueUp Job found for SourceID={CurrentIntegration.SourceID}");
            return;
        }

        if (alreadyCreatedInFileLog)
        {
            _log(LogLevel.Info, $"Matching TrueUp Job found: {matchingTrueUp} for SourceID={CurrentIntegration.SourceID}, but FileLog has already been created.");
            return;
        }

        _log(LogLevel.Info, $"Matching TrueUp Job found: {matchingTrueUp} for SourceID={CurrentIntegration.SourceID}");

        var queuesToInsert = new List<Queue>();
        foreach (APIEntity entity in _APIEntities)
        {
            DateTime? startDate = null, endDate = null;
            switch (matchingTrueUp.PeriodToRetrieve)
            {
                case AggregateInitializeSettings.PeriodEnum.Weekly:
                    startDate = DateTime.UtcNow.Date.AddDays(-int.Parse(_trueUpConfiguration.WeeklyOffsetStart));
                    endDate = DateTime.UtcNow.Date.AddDays(-int.Parse(_trueUpConfiguration.WeeklyOffsetEnd));
                    break;

                case AggregateInitializeSettings.PeriodEnum.Monthly:
                    startDate = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 1).AddMonths(-1);
                    var dayInMonth = DateTime.DaysInMonth(startDate.Value.Year, startDate.Value.Month);
                    endDate = new DateTime(startDate.Value.Year, startDate.Value.Month, dayInMonth);
                    break;

                case AggregateInitializeSettings.PeriodEnum.Single:
                    startDate = endDate = GetTriggerDate(matchingTrueUp.TriggerDayRegex, entity);
                    break;
            }

            if (!startDate.HasValue || !endDate.HasValue)
            {
                var startDateValue = startDate.HasValue ? startDate.Value.ToString("MM/dd/yyyy") : "null";
                var endDateValue = endDate.HasValue ? endDate.Value.ToString("MM/dd/yyyy") : "null";
                _log(LogLevel.Warn, $"Both startDate ({startDateValue}) and endDate ({endDateValue}) need a value");
                continue;
            }

            var dates = new List<DateTime>();
            for (DateTime date = startDate.Value; date <= endDate.Value; date = date.AddDays(1))
            {
                dates.Add(date);
            }

            var fileLogs = JobService.GetActiveAPIEntityFileLogs(entity.SourceID, entity.IntegrationID, entity.APIEntityCode, startDate.GetValueOrDefault());
            var alreadyCreatedFileLogs = fileLogs.Where(f => dates.Contains(f.FileDate) && f.IsBackfill);
            var datesExceptAlreadyInFileLog = dates.Except(alreadyCreatedFileLogs.Select(q => q.FileDate));

            var queues = CreateQueueItems(datesExceptAlreadyInFileLog, entity, BackFillType.DailyBackFill, "TrueUp");
            queuesToInsert.AddRange(queues);
            _log(LogLevel.Info, $"TrueUp Queued {dates.Count} reports for processing for Entity: {entity.APIEntityCode}");
        }

        _queueRepository.BulkInsert(queuesToInsert);
    }

    private static string GenerateDateExpression()
    {
        List<string> dateExpressionComponents = new List<string>();

        dateExpressionComponents.Add(DateTime.UtcNow.DayOfWeek.ToString().ToUpper());
        dateExpressionComponents.Add(DateTime.UtcNow.ToString("dd"));
        if (DateTime.DaysInMonth(DateTime.UtcNow.Year, DateTime.UtcNow.Month) == DateTime.UtcNow.Day)
        {
            dateExpressionComponents.Add("LASTDAYOFTHEMONTH");
        }

        string dateExpression = string.Join(" ", dateExpressionComponents);
        return dateExpression;
    }

    private sealed class QueueInfo
    {
        public QueueInfo(DateTime dateTime, bool isBackFill)
        {
            this.DateTime = dateTime;
            this.IsBackFill = isBackFill;
        }
        public DateTime DateTime { get; set; }
        public bool IsBackFill { get; set; }
    }

    /// <summary>
    /// returns the list of UTC dates (offset applied to UTCNow)  + if the queue for that date is Backfill or not
    /// </summary>
    /// <returns></returns>
    private List<QueueInfo> GetQueueInfoList()
    {
        //daily dates
        var queueInfo = new List<QueueInfo>();

        if (_aggregateInitializeSettings?.RegularAggregateDays?.Any() == true || _aggregateInitializeSettings?.BackFillAggregateDays?.Any() == true)
        {
            //get the date range that should be included base on (from yesterday to yesterday-DailyOffset)
            DateTime startDate = DateTime.UtcNow;
            if (_aggregateInitializeSettings.RegularAggregateDays != null)
            {
                foreach (int offset in _aggregateInitializeSettings.RegularAggregateDays)
                {
                    queueInfo.Add(new QueueInfo(startDate.AddDays(-offset), false));
                }
            }

            if (_aggregateInitializeSettings.BackFillAggregateDays != null)
            {
                foreach (int offset in _aggregateInitializeSettings.BackFillAggregateDays)
                {
                    queueInfo.Add(new QueueInfo(startDate.AddDays(-offset), true));
                }
            }
        }
        else
        {
            _log(LogLevel.Info, "No regular days set in [Source].[AggregateInitializeSettings]");
        }

        return queueInfo;
    }

    private List<Queue> CreateQueueItems(IEnumerable<DateTime> dates, APIEntity entity, BackFillType backFillType, string operationNameInLog)
    {
        bool isBackfill = backFillType == BackFillType.DailyBackFill || backFillType == BackFillType.ManualBackFill || backFillType == BackFillType.ManualDimensionBackFill;
        bool isDimOnly = backFillType == BackFillType.ManualDimensionBackFill;

        var queues = new List<Queue>();

        foreach (var fileDate in dates)
        {
            Queue queueItem = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                FileSize = 0,
                FileDate = fileDate,
                SourceFileName = $"{CurrentSource.SourceName}Reports",
                FileName = $"{CurrentSource.SourceName}Reports_{fileDate.ToString("yyyyMMdd")}_{entity.APIEntityCode}",
                IntegrationID = entity.IntegrationID,
                SourceID = CurrentSource.SourceID,
                Status = Constants.JobStatus.Pending.ToString(),
                StatusId = (int)Constants.JobStatus.Pending,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                Step = Constants.JobStep.Import.ToString(),
                EntityID = entity.APIEntityCode,
                IsBackfill = isBackfill,
                IsDimOnly = isDimOnly
            };

            queues.Add(queueItem);
            _log(LogLevel.Info, $"{this.JED.JobGUID} - Initialized - {operationNameInLog} - Queue item created in list - SourceID=SourceID={CurrentSource.SourceID} - EntityID={entity.APIEntityCode} - FileDate={fileDate} - isBackFill={isBackfill} - backfillType = {backFillType}");
        }

        return queues;
    }

    private Queue CreateQueueItems(DateTime date, APIEntity entity, BackFillType backFillType, string operationNameInLog)
    {
        var queues = CreateQueueItems(new List<DateTime> { date }, entity, backFillType, operationNameInLog);

        if (queues.Count != 1)
        {
            throw new AggregateInitializeException("Bug in the code: if 1 date is specified, 1 queue is created");
        }

        return queues.First();
    }

    private void ScheduleImportJob()
    {
        Greenhouse.Data.Model.Core.JobExecutionDetails newJED = base.CloneJED();
        newJED.Step = newJED.ExecutionPath.CurrentStep.Step.ParseEnum<Constants.JobStep>();
        newJED.JobProperties[Constants.US_SOURCE_ID] = CurrentSource.SourceID;

        foreach (var integration in _integrations.Where(i => i.IsActive))
        {
            newJED.ResetExecutionGuid();
            newJED.JobProperties[Constants.US_INTEGRATION_ID] = integration.IntegrationID;

            bool ckExists = CacheStore.Exists(newJED.JobCacheKey);
            _logger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Info, _logger.Name,
                string.Format("{2} - childJobCacheKey {0} {1}", newJED.JobCacheKey,
                    (ckExists ? "EXISTS" : "DOES NOT EXIST"), this.JED.JobGUID)));

            if (!ckExists)
            {
                _log(LogLevel.Debug, string.Format("{1} - Current Step is: {0} ", newJED.ExecutionPath.CurrentStep,
                    this.JED.JobGUID));
                base.ScheduleDynamicJob(newJED);
                _log(LogLevel.Debug, string.Format("{2} - Job {0} batched and scheduled for integration: {1}",
                    newJED.ExecutionPath.CurrentStep.SourceJobStepName, integration.IntegrationID,
                    this.JED.JobGUID));
            }
            else
            {
                _log(LogLevel.Debug, string.Format(
                    "{2} - Job SKIPPED for Source: {0} Integration: {1}. Already exists in job cache.",
                    CurrentSource.SourceName, integration.IntegrationName, this.JED.JobGUID));
            }
        }
    }

    private DateTime GetTriggerDate(string triggerDayRegex, APIEntity entity)
    {
        var today = ApplyTimeZoneToUtcDate(entity, DateTime.UtcNow).Date;

        // Check if input is a numeric day of the month
        if (int.TryParse(triggerDayRegex, out int dayOfMonth))
        {
            DateTime nextTriggerDate;

            if (dayOfMonth <= today.Day)
            {
                // If the day has already occurred in this month, return it
                nextTriggerDate = new DateTime(today.Year, today.Month, dayOfMonth);
            }
            else
            {
                // Otherwise, get the same day from the previous month
                DateTime previousMonth = today.AddMonths(-1);
                int lastDayOfPreviousMonth = DateTime.DaysInMonth(previousMonth.Year, previousMonth.Month);

                // Ensure we don't select an invalid date
                int validDay = Math.Min(dayOfMonth, lastDayOfPreviousMonth);
                nextTriggerDate = new DateTime(previousMonth.Year, previousMonth.Month, validDay);
            }

            return nextTriggerDate;
        }
        else
        {
            // Try parsing the weekday
            if (DateTime.TryParseExact(triggerDayRegex, "dddd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDay))
            {
                DayOfWeek targetDayOfWeek = parsedDay.DayOfWeek;
                if (today.DayOfWeek == targetDayOfWeek)
                {
                    return today;
                }

                // Otherwise, calculate the last occurrence of the weekday before today
                int daysSinceLastOccurrence = ((int)today.DayOfWeek - (int)targetDayOfWeek + 7) % 7;
                return today.AddDays(-daysSinceLastOccurrence);
            }
        }

        throw new ArgumentException("Invalid triggerDayRegex format.");
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
        }
    }

    ~AggregateInitializeJob()
    {
        Dispose(false);
    }

    public string GetJobCacheKey()
    {
        return DefaultJobCacheKey;
    }
}

[Serializable]
internal sealed class AggregateInitializeException : Exception
{
    public AggregateInitializeException()
    {
    }

    public AggregateInitializeException(string message) : base(message)
    {
    }

    public AggregateInitializeException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.NetBase;
using Greenhouse.Data.DataSource.NetBase.Core;
using Greenhouse.Data.DataSource.NetBase.Data.MetricValues;
using Greenhouse.Data.DataSource.NetBase.Data.Themes;
using Greenhouse.Data.DataSource.NetBase.Data.Topics;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.NetBase;

[Export("NetBase-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private ApiClient _apiClient;
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private RemoteAccessClient _remoteAccessClient;
    private Uri _baseDestUri;
    private Uri _baseStageDestUri;
    private Uri _baseLocalImportUri;
    private List<IFileItem> _queueItems;
    private IEnumerable<APIReport<MetricValuesParameters>> _metricValuesReports;
    private IEnumerable<APIReport<TopicsParameters>> _topicReports;
    private IEnumerable<APIReport<ThemesParameters>> _themeReports;
    private TopicsResponse _entityTopic;
    private NetBaseOAuth _netBaseOAuth;
    private NetBaseService _netBaseService;
    private List<NetBaseWebError> _netBaseWebErrors;
    private string _rateLimitHeader;
    private string _concurrentRateLimitHeader;
    private string _rateLimitRemainingHeader;
    private string _concurrentRateLimitRemainingHeader;
    private int _dailyReportLookback;
    private int _urlMaxLength;

    private string JobGUID
    {
        get { return this.JED.JobGUID.ToString(); }
    }

    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _baseLocalImportUri = GetLocalImportDestinationFolder();
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID)?.ToList();
        _remoteAccessClient = base.GetS3RemoteAccessClient();
        _metricValuesReports = JobService.GetAllActiveAPIReports<MetricValuesParameters>(base.SourceId).Where(x => x.ReportSettings.ReportType == ReportSettings.NetBaseApiMethods.metricValues);
        _topicReports = JobService.GetAllActiveAPIReports<TopicsParameters>(base.SourceId).Where(x => x.ReportSettings.ReportType == ReportSettings.NetBaseApiMethods.topics);
        _themeReports = JobService.GetAllActiveAPIReports<ThemesParameters>(base.SourceId).Where(x => x.ReportSettings.ReportType == ReportSettings.NetBaseApiMethods.themes);
        _netBaseService = new NetBaseService();
        _netBaseOAuth = new NetBaseOAuth(base.CurrentCredential, _httpClientProvider);
        var netBaseLookupError = SetupService.GetById<Lookup>(Constants.NETBASE_WEB_ERRORS);
        _netBaseWebErrors = string.IsNullOrEmpty(netBaseLookupError?.Value) ? new List<NetBaseWebError>() : ETLProvider.DeserializeType<List<NetBaseWebError>>(netBaseLookupError.Value);
        _rateLimitHeader = SetupService.GetById<Lookup>(Constants.NETBASE_RATE_LIMIT_HEADER)?.Value ?? "X-RateLimit-Reset";
        _concurrentRateLimitHeader = SetupService.GetById<Lookup>(Constants.NETBASE_CONCURRENT_RATE_LIMIT_HEADER)?.Value ?? "X-ConcurrentRateLimit-Reset";
        _rateLimitRemainingHeader = SetupService.GetById<Lookup>(Constants.NETBASE_RATE_LIMIT_REMAINING_HEADER)?.Value ?? "X-RateLimit-Remaining";
        _concurrentRateLimitRemainingHeader = SetupService.GetById<Lookup>(Constants.NETBASE_CONCURRENT_RATE_LIMIT_REMAINING_HEADER)?.Value ?? "X-ConcurrentRateLimit-Remaining";
        var dailyReportLookup = SetupService.GetById<Lookup>(Constants.NETBASE_DAILY_REPORT_LOOKBACK);
        _dailyReportLookback = string.IsNullOrEmpty(dailyReportLookup?.Value) ? 14 : int.Parse(dailyReportLookup.Value);
        var urlMaxLengthLookup = SetupService.GetById<Lookup>(Constants.NETBASE_URL_MAX_LENGTH);
        _urlMaxLength = string.IsNullOrEmpty(urlMaxLengthLookup?.Value) ? 8192 : int.Parse(urlMaxLengthLookup.Value);
        _apiClient = new(_httpClientProvider, CurrentIntegration, _netBaseOAuth, _urlMaxLength);
    }

    public void Execute()
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE START {base.DefaultJobCacheKey}")));
        var exceptions = new List<System.Tuple<System.Guid, string>>();
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.NETBASE_POLLY_MAX_RETRY)?.Value, out int maxRetry))
            maxRetry = 10;
        var apiCallsBackOffStrategy = new BackOffStrategy() { Counter = 0, MaxRetry = maxRetry };
        var netBasePolicy = GetPollyRetryNetBasePolicy(this.JobGUID, apiCallsBackOffStrategy);

        if (_metricValuesReports.Any())
        {
            var badEntityList = new List<string>();
            if (_queueItems.Count != 0)
            {
                foreach (Queue queueItem in _queueItems)
                {
                    var reportItems = new List<ApiReportItem>();

                    if (badEntityList.Contains(queueItem.EntityID)) continue;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                    try
                    {
                        CleanupLocalEntityFolder(queueItem);

                        //get topic metadata for specific Entity ID
                        var topics = GenerateReports(_topicReports, netBasePolicy, queueItem, reportItems);

                        //set topic ID here as the entity ID
                        _entityTopic = topics.First(t => t.TopicId == queueItem.EntityID);

                        //get list of theme IDs from a Lookup value (ie NETBASE-<topicID>)
                        //product user will add/remove theme IDs for each topic from the Lookup-Setup in the UI
                        var themes = GetTopicThemes(Constants.NETBASE_TOPIC_THEMES_PREFIX, _entityTopic.TopicId);
                        GenerateReports(_themeReports, netBasePolicy, queueItem, themes, reportItems);

                        //get metric-values reports
                        //the endpoint for metric-values only includes breakdown by metric-series in the output
                        //therefore a separate report is set up for each combination of series, domain and theme
                        if (themes.Count != 0)
                        {
                            GenerateReports(_metricValuesReports, netBasePolicy, queueItem, themes, reportItems);
                        }
                        else
                        {
                            GenerateReports(_metricValuesReports, netBasePolicy, queueItem, reportItems);
                        }

                        //convert json files to a format compatible with Redshift
                        //and upload to stage folder in s3 for processing
                        //local raw json files are archived in a tar.gz file and uploaded to raw s3 folder
                        StageReport(queueItem, exceptions, reportItems);
                    }
                    catch (HttpClientProviderRequestException exc)
                    {
                        HandleException(
                            exceptions,
                            badEntityList,
                            queueItem,
                            Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Error with queue item -> failed on: {queueItem.FileGUID} " +
                                $"for EntityID: {queueItem.EntityID} " +
                                $"FileDate: {queueItem.FileDate} -> " +
                                $"Exception details : {exc}"), exc), exc.Message);
                    }
                    catch (Exception exc)
                    {
                        HandleException(
                            exceptions,
                            badEntityList,
                            queueItem,
                            Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Error with queue item -> failed on: {queueItem.FileGUID} " +
                                $"for EntityID: {queueItem.EntityID} " +
                                $"FileDate: {queueItem.FileDate} -> " +
                                $"Exception: {exc.Message} - " +
                                $"STACK {exc.StackTrace}"), exc), exc.Message);
                    }
                }
            }
            else
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("No items in the Queue")));
            }
        }
        else
        {
            var missingReportException = new APIReportException($"No API Reports found");
            exceptions.Add(Tuple.Create(this.JED.JobGUID, missingReportException.Message));
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Import job error ->  Exception: {missingReportException.Message}")));
        }

        if (exceptions.Count > 0)
        {
            throw new ErrorsFoundException($"Total errors: {exceptions.Count}; Please check Splunk for more detail.");
        }

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE END {base.DefaultJobCacheKey}")));
    }

    private void HandleException(List<Tuple<Guid, string>> exceptions, List<string> badEntityList, Queue queueItem, LogEventInfo logEventInfo, string exceptionMsg)
    {
        UpdateQueueAndLogError(exceptions, queueItem, logEventInfo, exceptionMsg);

        var unfinishedQueues = _queueItems.Where(q =>
                q.EntityID.Equals(queueItem.EntityID, StringComparison.InvariantCultureIgnoreCase) &&
                q.Status != Constants.JobStatus.Complete.ToString())
            .Select(x => x.ID)
            .Distinct()
            .Select(x => new Queue { ID = x });

        UpdateQueueWithDelete(unfinishedQueues, Common.Constants.JobStatus.Error, false);
        badEntityList.Add(queueItem.EntityID);
    }
    private static void UpdateQueueAndLogError(List<Tuple<Guid, string>> exceptions, Queue queueItem, LogEventInfo logEventInfo, string exceptionMsg)
    {
        exceptions.Add(Tuple.Create(queueItem.FileGUID, exceptionMsg));
        queueItem.Status = Constants.JobStatus.Error.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        _logger.Log(logEventInfo);
    }

    private void GenerateReports(IEnumerable<APIReport<MetricValuesParameters>> reports, RetryPolicy netBasePolicy, Queue queueItem, List<ApiReportItem> reportItems)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic id {queueItem.EntityID} - starting Metric Values data download")));
        var counter = 0;
        foreach (var report in reports)
        {
            var apiReportItem = APICallGenerateReport(netBasePolicy, queueItem, report, ref counter);

            reportItems.Add(apiReportItem);

            UpdateFileCollection(queueItem, apiReportItem);
        }

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Successful Metric Values data download for Topic ID {queueItem.EntityID}; Call Summary: total calls made: {counter}")));
    }

    private void GenerateReports(IEnumerable<APIReport<MetricValuesParameters>> reports, RetryPolicy netBasePolicy, Queue queueItem, List<string> topicThemeIdList, List<ApiReportItem> reportItems)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic id {queueItem.EntityID} - starting Metric Values data download")));
        var counter = 0;
        foreach (var report in reports)
        {
            foreach (var themeId in topicThemeIdList)
            {
                var apiReportItem = APICallGenerateReport(netBasePolicy, queueItem, report, ref counter, themeId);

                reportItems.Add(apiReportItem);

                UpdateFileCollection(queueItem, apiReportItem);
            }
        }

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Successful Metric Values data download for Topic ID {queueItem.EntityID}; Call Summary: total calls made: {counter}")));
    }

    private ApiReportItem APICallGenerateReport(RetryPolicy netBasePolicy, Queue queueItem, APIReport<MetricValuesParameters> report,
        ref int counter, string themeId = null)
    {
        //create local file to store raw json
        string theme = string.IsNullOrEmpty(themeId) ? string.Empty : $"{themeId}_";
        string[] paths =
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate),
            $"{queueItem.FileGUID}_{report.ReportSettings.ReportType}_{report.ReportSettings.ReportMetric}_{theme}{counter}_{queueItem.FileDate:yyyy-MM-dd}.json"
        };

        var localFile = CreateLocalFile(queueItem, paths);

        //set topic
        report.ReportSettings.TopicIDs = queueItem.EntityID;

        //set theme
        report.ReportSettings.ThemeIds = themeId;

        //the netbase API response has date and values in an indexed array (ie small file size)
        //the wait time between requesting 8 days versus 30 days worth of data is negligible
        //therefore we will have one queue record pulling in data x days back to reduce api calls
        //default is to set the start and end date range using the lookup values
        //start-date => offset number of days back from the file date (offset is taken from the lookup)
        //end-date => file date + 1 because the end-date is exclusive

        string startDate = null, endDate = null;
        bool makeAPICall = true;

        if (queueItem.IsBackfill)
        {
            makeAPICall = SetDatesForBackfill(queueItem, ref startDate, ref endDate);
        }
        else
        {
            var offsetDays = _dailyReportLookback;

            startDate = ConvertToUTCString(queueItem.FileDate.AddDays(-offsetDays));
            //publish end date is exclusive so we request a day ahead of what we want
            endDate = ConvertToUTCString(queueItem.FileDate.AddDays(1));
        }

        if (!queueItem.IsBackfill && (!string.IsNullOrEmpty(report.ReportSettings.TimePeriod) ||
                  !string.IsNullOrEmpty(report.ReportSettings.DateRange)) && !queueItem.IsBackfill)
        {
            //nullify dates when time period or date range is used in report settings
            //and it is a daily job
            startDate = null;
            endDate = null;
        }

        if (makeAPICall)
        {
            netBasePolicy.Execute(
                (_) => DownloadReport(queueItem, report, localFile, report.ReportSettings.URLPath, startDate, endDate),
                new Dictionary<string, object> { { "methodName", "DownloadReport" } });
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid($"Data has been saved to a local file at {localFile.FullName}; size={localFile.Length}")));
        }
        else
        {
            // not making a call, 
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid($"Not calling the API, creating an empty file at {localFile.FullName}")));
            CreateEmptyReport(localFile);
        }

        var apiReportItem = new ApiReportItem
        {
            ReportName = localFile.Name,
            ReportSize = localFile.Length,
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            TopicID = queueItem.EntityID,
            ThemeID = themeId,
            ReportType = report.ReportSettings.ReportType.ToString(),
            ReportMetric = report.ReportSettings.ReportMetric
        };

        counter++;

        return apiReportItem;
    }

    private bool SetDatesForBackfill(Queue queueItem, ref string startDate, ref string endDate)
    {
        bool makeAPICall = true;

        var start = new DateTime(queueItem.FileDate.Year, queueItem.FileDate.Month, 01).Date;
        //publish end date is exclusive so we request a day ahead of what we want
        var end = start.AddMonths(1);

        var timeInterval =
            (NetBaseEnums.DateRangeEnum)Enum.Parse(typeof(NetBaseEnums.DateRangeEnum),
                _entityTopic.TimeInterval);
        var maxStartDate = GetMaxStartTime(DateTime.UtcNow, timeInterval);

        if (end.CompareTo(maxStartDate) < 0)
        {
            // end date is earlier than MaxStartDate
            // no data will be returned
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                PrefixJobGuid(
                    $"Queue ID={queueItem.ID} - FileGUID='{queueItem.FileGUID}' End Date ({endDate}) < maxStart Date ({maxStartDate}) - no call made (empty file) for Topic Id ({_entityTopic.TopicId}) Name '{_entityTopic.Name}' - Interval '{_entityTopic.TimeInterval}'")));
            makeAPICall = false;
        }
        else if (start.CompareTo(maxStartDate) < 0)
        {
            // start date is earlier than maxStartDate
            // updating startDate to MaxStartDate as it is the maximum startDate allowed
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                PrefixJobGuid(
                    $"Queue ID={queueItem.ID} - FileGUID='{queueItem.FileGUID}' Start Date ({startDate}) < maxStart Date ({maxStartDate}) - updating start date to ({maxStartDate}) for Topic Id ({_entityTopic.TopicId}) Name '{_entityTopic.Name}' - Interval '{_entityTopic.TimeInterval}'")));
            start = maxStartDate;
        }

        startDate = ConvertToUTCString(start);
        endDate = ConvertToUTCString(end);
        return makeAPICall;
    }

    private static string ConvertToUTCString(DateTime date) => $"{date:yyyy-MM-ddT00:00:00Z}";

    private static DateTime GetMaxStartTime(DateTime date, NetBaseEnums.DateRangeEnum period)
    {
        DateTime maxStartTime = DateTime.UtcNow;
        switch (period)
        {
            case NetBaseEnums.DateRangeEnum.LAST_HOUR:
                maxStartTime = date.AddHours(-1);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_DAY:
                maxStartTime = date.AddDays(-1);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_WEEK:
                maxStartTime = date.AddDays(-7);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_MONTH:
                maxStartTime = date.AddMonths(-1);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_QUARTER:
                maxStartTime = date.AddMonths(-3);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_SIX_MONTHS:
                maxStartTime = date.AddMonths(-6);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_YEAR:
                maxStartTime = date.AddYears(-1);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_2YEARS:
                maxStartTime = date.AddYears(-2);
                break;
            case NetBaseEnums.DateRangeEnum.LAST_27MONTHS:
                maxStartTime = date.AddMonths(-27);
                break;
            default:
                throw new NotSupportedException($"NetBaseEnums.DateRangeEnum {period} not handled in method GetMinStartTime");
        }

        return maxStartTime;
    }

    private List<TopicsResponse> GenerateReports(IEnumerable<APIReport<TopicsParameters>> reports, RetryPolicy netBasePolicy, Queue queueItem, List<ApiReportItem> reportItems)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic id {queueItem.EntityID} - starting Topics data download")));
        var counter = 0;
        var topicsList = new List<TopicsResponse>();
        foreach (var report in reports)
        {
            //create local file to store raw json
            string[] paths =
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), $"{queueItem.FileGUID}_{report.ReportSettings.ReportType}_{counter}_{queueItem.FileDate:yyyy-MM-dd}.json"
            };
            var localFile = CreateLocalFile(queueItem, paths);

            //set topic
            report.ReportSettings.Ids = new List<string>() { queueItem.EntityID };

            var reportResponse = netBasePolicy.Execute((_) => DownloadTopicReport(queueItem, report, localFile, report.ReportSettings.URLPath), new Dictionary<string, object> { { "methodName", "DownloadTopicReport" } });

            var deserializedJson = JsonConvert.DeserializeObject<List<TopicsResponse>>(reportResponse);
            topicsList.AddRange(deserializedJson);

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Data has been saved to a local file at {localFile.FullName}; size={localFile.Length}")));

            var apiReportItem = new ApiReportItem
            {
                ReportName = localFile.Name,
                ReportSize = localFile.Length,
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportType = report.ReportSettings.ReportType.ToString(),
                ReportMetric = report.ReportSettings.ReportMetric.ToString()
            };

            reportItems.Add(apiReportItem);

            UpdateFileCollection(queueItem, apiReportItem);
            counter++;
        }

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Successful Topics data download for Topic ID {queueItem.EntityID}; Call Summary: total calls made: {counter}")));

        return topicsList;
    }

    private List<ThemesResponse> GenerateReports(IEnumerable<APIReport<ThemesParameters>> reports, RetryPolicy netBasePolicy, Queue queueItem, List<string> topicThemesList, List<ApiReportItem> reportItems)
    {
        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic id {queueItem.EntityID} - starting download of this topic's themes metadata")));
        var counter = 0;
        var themesList = new List<ThemesResponse>();
        foreach (var report in reports)
        {
            //save a copy of original report theme ids to reset the reference at end of for-loop
            var themeIDs = report.ReportSettings.Ids == null ? null : new List<string>();

            //you can download a specific theme ID via parameter "ids" (ie ids=<theme-id>)
            //if this "ids" parameter is null in API Reports then we will use the theme IDs list provided in the Lookup
            //if this "ids" parameter has value "ALL" then we will set parameter to null and not use it in the request (ie get all topic IDs)
            if (report.ReportSettings.Ids != null)
            {
                if (report.ReportSettings.Ids.Count == 0 || report.ReportSettings.Ids.Any(x => x.Equals("ALL", StringComparison.InvariantCultureIgnoreCase)))
                {
                    themeIDs.AddRange(report.ReportSettings.Ids);
                    report.ReportSettings.Ids = null;
                }
            }
            else if (report.ReportSettings.Ids == null && topicThemesList.Count != 0)
            {
                report.ReportSettings.Ids = topicThemesList;
            }

            //create local file to store raw json
            string[] paths =
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), $"{queueItem.FileGUID}_{report.ReportSettings.ReportType}_{counter}_{queueItem.FileDate:yyyy-MM-dd}.json"
            };
            var localFile = CreateLocalFile(queueItem, paths);

            var reportResponse = netBasePolicy.Execute((_) => DownloadThemesReport(queueItem, report, localFile, report.ReportSettings.URLPath), new Dictionary<string, object> { { "methodName", "DownloadThemesReport" } });

            var deserializedJson = JsonConvert.DeserializeObject<List<ThemesResponse>>(reportResponse);
            themesList.AddRange(deserializedJson);

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Data has been saved to a local file at {localFile.FullName}; size={localFile.Length}")));

            var apiReportItem = new ApiReportItem
            {
                ReportName = localFile.Name,
                ReportSize = localFile.Length,
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportType = report.ReportSettings.ReportType.ToString(),
                ReportMetric = report.ReportSettings.ReportMetric.ToString()
            };

            reportItems.Add(apiReportItem);

            UpdateFileCollection(queueItem, apiReportItem);
            counter++;

            report.ReportSettings.Ids = themeIDs;
        }

        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Successful Themes data download for Topic ID {queueItem.EntityID}; Call Summary: total calls made: {counter}")));

        return themesList;
    }

    private static void UpdateFileCollection(Queue queueItem, ApiReportItem apiReportItem)
    {
        var files = queueItem.FileCollection?.ToList() ?? new List<FileCollectionItem>();

        FileCollectionItem fileItem = new FileCollectionItem()
        {
            FileSize = apiReportItem.ReportSize,
            SourceFileName = apiReportItem.ReportType,
            FilePath = apiReportItem.ReportName
        };
        files.Add(fileItem);
        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
        queueItem.FileSize += apiReportItem.ReportSize;
    }

    private ReportResponse DownloadReport<T>(Queue queueItem, APIReport<T> report, FileSystemFile localFile, string netBaseMethod, string startDate = null, string endDate = null)
    {
        var options = new FetchDataOptions()
        {
            ReportParameters = report.ReportSettings,
            UrlExtension = netBaseMethod,
            StartDate = startDate,
            EndDate = endDate

        };
        var reportResponse = _apiClient.FetchDataAsync<ReportResponse>(options).GetAwaiter().GetResult();

        using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
        {
            output.Write(reportResponse.RawJson);
        }
        //during unit testing, the time in seconds in the concurrent-rate-limit header cannot be trusted
        //creating a lookup that can override this time
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.NETBASE_CONCURRENT_RATE_LIMIT_OVERRIDE)?.Value, out int concurrentRateLimitOverride))
            concurrentRateLimitOverride = int.Parse(reportResponse.Header[$"{_concurrentRateLimitHeader}"]);

        //To avoid a 403 error, ensure that the number of queries you submit does not exceed the rate limit. By default, the maximum number of queries you can run with the Insight API is:
        //  50 queries per hour
        //  One query per minute
        if (int.Parse(reportResponse.Header[$"{_rateLimitRemainingHeader}"]) == 0)
        {
            var secondsRemaining = int.Parse(reportResponse.Header[$"{_rateLimitHeader}"]);
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic {queueItem.EntityID} - report {report.APIReportName} request has reached the maximum calls per hour, will make next call in {secondsRemaining} seconds")));
            Task.Delay(secondsRemaining * 1000).Wait();
        }
        else if (int.Parse(reportResponse.Header[$"{_concurrentRateLimitRemainingHeader}"]) == 0)
        {
            var secondsRemaining = concurrentRateLimitOverride;
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Topic {queueItem.EntityID} - report {report.APIReportName} request has reached the maximum calls per minute, will make next call in {secondsRemaining} seconds")));
            Task.Delay(secondsRemaining * 1000).Wait();
        }

        return reportResponse;
    }

    private static void CreateEmptyReport(FileSystemFile localFile)
    {
        using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
        {
            output.Write("{}");
        }
    }

    private string DownloadTopicReport(Queue queueItem, APIReport<TopicsParameters> report, FileSystemFile localFile, string netBaseMethod)
    {
        var options = new FetchDataOptions()
        {
            ReportParameters = report.ReportSettings,
            UrlExtension = netBaseMethod,

        };
        var reportResponse = _apiClient.FetchRawDataAsync(options).GetAwaiter().GetResult();

        using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
        {
            output.Write(reportResponse);
        }

        return reportResponse;
    }

    public static List<string> GetTopicThemes(string type, string topicId)
    {
        string key = type + topicId;
        Lookup themeIds = SetupService.GetById<Lookup>(key);

        if (themeIds == null)
        {
            return new List<string>();
        }

        var topicThemes = JsonConvert.DeserializeObject<List<string>>(themeIds.Value);

        return topicThemes;
    }

    private string DownloadThemesReport(Queue queueItem, APIReport<ThemesParameters> report, FileSystemFile localFile, string netBaseMethod)
    {
        var options = new FetchDataOptions()
        {
            ReportParameters = report.ReportSettings,
            UrlExtension = netBaseMethod,

        };
        var reportResponse = _apiClient.FetchRawDataAsync(options).GetAwaiter().GetResult();

        using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
        {
            output.Write(reportResponse);
        }

        return reportResponse;
    }

    private FileSystemFile CreateLocalFile(Queue queueItem, string[] paths)
    {
        Uri localFileUri = RemoteUri.CombineUri(_baseLocalImportUri, paths);

        var localFile = new FileSystemFile(localFileUri);
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
        }

        return localFile;
    }

    private T GetReportData<T>(string[] paths, bool isLocal = false, Newtonsoft.Json.JsonSerializerSettings jsonSettings = null) where T : new()
    {
        var reportData = new T();
        if (jsonSettings is null)
        {
            jsonSettings = new Newtonsoft.Json.JsonSerializerSettings()
            {
                MissingMemberHandling = MissingMemberHandling.Ignore,
                NullValueHandling = NullValueHandling.Ignore
            };
        }

        var sourceUri = isLocal ? this._baseLocalImportUri : this._baseDestUri;
        var filePath = RemoteUri.CombineUri(sourceUri, paths);

        using (var sourceStream = isLocal ? File.OpenRead(filePath.AbsolutePath) : _remoteAccessClient.WithFile(filePath).Get())
        {
            using (var txtReader = new StreamReader(sourceStream))
            {
                var deserializedJson = JsonConvert.DeserializeObject<T>(txtReader.ReadToEnd());
                if (deserializedJson != null)
                {
                    reportData = deserializedJson;
                }
            }
        }

        return reportData;
    }

    private void WriteObjectToFile(JArray entity, string entityID, DateTime fileDate, string filename)
    {
        string[] paths = new string[]
        {
            entityID.ToLower(), GetDatedPartition(fileDate), filename
        };

        IFile transformedFile = _remoteAccessClient.WithFile(RemoteUri.CombineUri(_baseStageDestUri, paths));
        ETLProvider.SerializeRedshiftJson(entity, transformedFile);
    }

    private void StageReport(Queue queueItem, List<Tuple<Guid, string>> exceptions, List<ApiReportItem> reportItems)
    {
        try
        {
            if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    PrefixJobGuid(
                        $"File Collection is empty; unable to stage data for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} ")));
            }
            else
            {
                Action<JArray, string, DateTime, string> writeToFileSignature = ((a, b, c, d) => WriteObjectToFile(a, b, c, d));

                foreach (var report in reportItems)
                {
                    //locally saved files use the filedate in their filepath
                    string[] paths = new string[]
                    {
                        queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.ReportName
                    };

                    _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                        PrefixJobGuid(
                            $"Staging Dimension Report for raw file: {report.ReportName}; report type {report.ReportType}; account id: {report.TopicID}; file date: {queueItem.FileDate}; fileGUID: {report.FileGuid}")));

                    var reportType = Utilities.UtilsText.ConvertToEnum<ReportSettings.NetBaseApiMethods>(report.ReportType);

                    switch (reportType)
                    {
                        case ReportSettings.NetBaseApiMethods.metricValues:
                            var metricValuesData = GetReportData<MetricValuesResponse>(paths, true);
                            NetBaseService.StageMetricSeries(queueItem.EntityID, report, queueItem.FileDate, metricValuesData, writeToFileSignature);
                            break;
                        case ReportSettings.NetBaseApiMethods.topics:
                            var topicData = GetReportData<List<TopicsResponse>>(paths, true);
                            _netBaseService.StageTopics(queueItem.EntityID, report, queueItem.FileDate, topicData, writeToFileSignature);
                            break;
                        case ReportSettings.NetBaseApiMethods.themes:
                            var themeData = GetReportData<List<ThemesResponse>>(paths, true);
                            _netBaseService.StageThemes(queueItem.EntityID, report, queueItem.FileDate, themeData, writeToFileSignature);
                            break;
                    }
                }
            }

            //archive all "raw" json files that were stored locally in a tar.gz file and upload to raw folder in s3
            ArchiveRawFiles(queueItem, queueItem.FileDate, "rawJSON");

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid($"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}")));
            queueItem.Status = Constants.JobStatus.Complete.ToString();
            queueItem.StatusId = (int)Constants.JobStatus.Complete;
            JobService.Update(queueItem);
        }
        catch (HttpClientProviderRequestException exc)
        {
            UpdateQueueAndLogError(exceptions, queueItem, Msg.Create(LogLevel.Error, _logger.Name,
                base.PrefixJobGuid(
                    $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} " +
                    $"FileDate: {queueItem.FileDate} -> Exception details : {exc}")
            , exc), exc.Message);
        }
        catch (Exception exc)
        {
            UpdateQueueAndLogError(exceptions, queueItem, Msg.Create(LogLevel.Error, _logger.Name,
                base.PrefixJobGuid(
                    $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.GetType().FullName} - Message: {exc.Message} - STACK {exc.StackTrace}")
            , exc), exc.Message);
        }
    }

    private void ArchiveRawFiles(Queue queueItem, DateTime importDateTime, string fileType)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Start archiving raw json data")));

        var entityID = queueItem.EntityID.ToLower();
        Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, entityID);
        var sourceDirectory = tempLocalImportUri.AbsolutePath;

        string[] destPaths = new string[]
        {
            entityID, $"{queueItem.FileGUID}_{entityID}_{fileType}_{importDateTime:yyyy-MM-dd}.tar.gz"
        };
        var archivePath = RemoteUri.CombineUri(_baseLocalImportUri, destPaths);
        var archiveFile = new FileSystemFile(archivePath);

        using (var stream = archiveFile.Create())
        {
            using (var gz = new GZipOutputStream(stream))
            {
                gz.SetLevel(3);
                using (var tar = new TarOutputStream(gz, Encoding.UTF8))
                {
                    var directories = Directory.GetDirectories(sourceDirectory);
                    foreach (string directory in directories)
                    {
                        var pathToCurrentDirectory = Path.Combine(sourceDirectory, directory);
                        string[] filenames = Directory.GetFiles(pathToCurrentDirectory);

                        foreach (string filename in filenames)
                        {
                            using (FileStream inputStream = File.OpenRead(filename))
                            {
                                string tarName = filename.Substring(3); // strip off "C:\"
                                long fileSize = inputStream.Length;
                                TarEntry entry = TarEntry.CreateTarEntry(tarName);
                                entry.Size = fileSize;
                                tar.PutNextEntry(entry);

                                byte[] localBuffer = new byte[32 * 1024];
                                while (true)
                                {
                                    int numRead = inputStream.Read(localBuffer, 0, localBuffer.Length);
                                    if (numRead <= 0)
                                        break;
                                    tar.Write(localBuffer, 0, numRead);
                                }
                            }
                            tar.CloseEntry();
                        }
                    }
                    tar.Close();
                }
                gz.Close();
            }
            stream.Close();
        }

        string[] s3paths = new string[]
        {
              entityID, GetDatedPartition(importDateTime), $"{queueItem.FileGUID}_{entityID}_{fileType}_{importDateTime:yyyy-MM-dd}.tar.gz"
        };
        S3File s3archiveFile = new S3File(RemoteUri.CombineUri(this._baseDestUri, s3paths), GreenhouseS3Creds);
        base.UploadToS3(archiveFile, s3archiveFile, s3paths);
        CleanupLocalEntityFolder(queueItem);
    }

    private void CleanupLocalEntityFolder(Queue queueItem)
    {
        Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, queueItem.EntityID.ToLower());
        FileSystemDirectory localImportDirectory = new FileSystemDirectory(tempLocalImportUri);
        if (localImportDirectory.Exists)
        {
            localImportDirectory.Delete(true);
        }
    }

    protected RetryPolicy GetPollyRetryNetBasePolicy(string fileGUID, IBackOffStrategy backOff)
    {
        return Policy.Handle<WebException>(a => _netBaseWebErrors.Any(e => RetrieveHttpStatusCode(a) == e.HttpStatusCode))
            .WaitAndRetry(
                retryCount: backOff.MaxRetry,
                sleepDurationProvider: (retryAttempt, response, context) =>
                {
                    //reset token
                    _netBaseOAuth.ResetAccessToken();

                    if (context.ContainsKey("webExceptionMessage"))
                    {
                        context["webExceptionMessage"] = response.Message;
                    }
                    else
                    {
                        context.Add("webExceptionMessage", response.Message);
                    }

                    var headers = ((System.Net.WebException)response).Response.Headers;
                    var allHeaders = string.Join("&", headers.AllKeys.Select(key => $"{key}={headers[key]}").ToList());
                    if (context.ContainsKey("responseHeaders"))
                    {
                        context["responseHeaders"] = allHeaders;
                    }
                    else
                    {
                        context.Add("responseHeaders", allHeaders);
                    }

                    //X-RateLimit-Reset: When you have reached your rate limit, the number of seconds until the Insight API restores a portion of your per-hour quota*
                    //*The Reset value is only meaningful when you have reached your query limit. It specifies the number of seconds until another portion of your quota is available
                    //, not the number of seconds until all of your per-minute or per-hour quota is available. For example, if your quota is 50 queries and the Remaining value reads 0
                    //, the Reset value indicates how many seconds you must wait for the Remaining value to increase. 

                    //check per-hour quota first
                    if (((System.Net.WebException)response).Response.Headers[$"{_rateLimitHeader}"] == null)
                        return backOff.GetNextTime();

                    var rateLimitRemaining = int.Parse(((System.Net.WebException)response).Response.Headers[$"{_rateLimitRemainingHeader}"]);
                    var secondsReset = int.Parse(((System.Net.WebException)response).Response.Headers[$"{_rateLimitHeader}"]);

                    if (rateLimitRemaining == 0)
                    {
                        return TimeSpan.FromSeconds(secondsReset);
                    }

                    //X-ConcurrentRateLimit-Reset: When you have reached your rate limit, the number of seconds until the Insight API restores a portion of your per-minute quota*
                    //check per-minute quota second
                    if (((System.Net.WebException)response).Response.Headers[$"{_concurrentRateLimitHeader}"] == null)
                        return backOff.GetNextTime();

                    var rateLimitRemainingConcurrent = int.Parse(((System.Net.WebException)response).Response.Headers[$"{_concurrentRateLimitRemainingHeader}"]);
                    var secondsResetConcurrent = int.Parse(((System.Net.WebException)response).Response.Headers[$"{_concurrentRateLimitHeader}"]);

                    if (rateLimitRemainingConcurrent == 0)
                    {
                        return TimeSpan.FromSeconds(secondsResetConcurrent);
                    }

                    return backOff.GetNextTime();
                },
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                        string.Format(
                            "{3}-{4}.Job error from web: {0} with Exception: {1}. Backoff Policy retry attempt: {2} -- headers: {5}",
                            context["webExceptionMessage"], exception.Message, retryCount, JED.JobGUID.ToString(),
                            fileGUID, context["responseHeaders"]), exception));
                }
            );
    }

    private static int RetrieveHttpStatusCode(WebException wex)
    {
        int statusCode = default(int);

        if (wex?.Response is HttpWebResponse httpWebResponse)
        {
            statusCode = (int)httpWebResponse.StatusCode;
        }

        return statusCode;
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

    ~ImportJob()
    {
        Dispose(false);
    }
}

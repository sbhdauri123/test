using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.DAL.DataSource.Reddit;
using Greenhouse.Data.DataSource.Reddit;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json.Linq;
using NLog;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using TimeZoneConverter;

namespace Greenhouse.Jobs.Aggregate.Reddit
{
    [Export("Reddit-AggregateImportJob", typeof(IDragoJob))]
    public class ImportJob : Framework.BaseFrameworkJob, IDragoJob
    {
        // Constant used in the DB
        private readonly CompositeFormat TIMEZONE_KEY = CompositeFormat.Parse("timezoneid_{0}"); // used in APIreport.APIReportSettings
        private const string REPORT_TYPE_METRICS = "metrics"; // used in APIreport.APIReportSettings
        private const string REPORT_TYPE_DIMENSION = "dimensions"; // used in APIreport.APIReportSettings
        private const string ACCOUNT_REPORT_NAME = "AccountsDim"; // used in APIreport.APIReportSettings

        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private Action<LogLevel, string> _log;
        private Action<LogLevel, string, Exception> _logEx;
        private RedditOAuth _oAuth;
        private List<OrderedQueue> _queueItems;
        private IEnumerable<APIReport<ReportSettings>> _apiReports;
        private Uri _baseRawDestUri;
        private int _exceptionCount;
        private int _warningCount;
        private readonly Stopwatch _runtime = new();
        private TimeSpan _maxRuntime;
        private int _pageSize;
        private readonly string _timezoneMissingException = "Failure: timezone missing from the job cache";
        private readonly HashSet<string> _downloadedDimReports = new();
        // cache the dimension cache reports
        private readonly Dictionary<string, List<FileCollectionItem>> _dimensionCache = new();
        // cache dim values used for later calls (example timezone from adaccount dim call used for the delivery report
        private readonly Dictionary<string, string> _dimensionValuesCache = new();
        private BaseApiProvider _apiProvider;
        private int _maxDegreeOfParallelism;
        private readonly List<string> _invalidAccountIds = new();
        private int _callsPerSecond;
        private int _fixedWindowInMilliseconds;

        public void PreExecute()
        {
            _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
            _logEx = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex));
            Stage = Constants.ProcessingStage.RAW;
            Initialize();
            _oAuth = new RedditOAuth(CurrentCredential, base.HttpClientProvider);

            _log(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

            int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
            _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)?.ToList();

            _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(SourceId);
            _baseRawDestUri = GetDestinationFolder();

            _pageSize = LookupService.GetLookupValueWithDefault(Constants.REDDIT_PAGESIZE, 500);
            _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.REDDIT_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
            _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.REDDIT_API_MAX_DEGREE_OF_PARALLELISM, 3);

            #region API Retry Pipeline
            int maxRetry = LookupService.GetLookupValueWithDefault(Constants.REDDIT_POLLY_MAX_RETRY, 10);
            int retryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.REDDIT_BACKOFF_DELAY_TIME_IN_SECONDS, 3);
            int maxRetryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.REDDIT_BACKOFF_DELAY_TIME_IN_SECONDS_MAX, 10);
            bool retryUseJitter = LookupService.GetLookupValueWithDefault(Constants.REDDIT_BACKOFF_USE_JITTER, true);

            var optionsOnRetry = new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().HandleResult((HttpResponseMessage r) => !r.IsSuccessStatusCode)
                    .Handle<HttpRequestException>(),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = retryUseJitter,
                Delay = TimeSpan.FromSeconds(retryDelayInSeconds),
                MaxRetryAttempts = maxRetry,
                MaxDelay = TimeSpan.FromSeconds(maxRetryDelayInSeconds),
                OnRetry = args =>
                {
                    _logEx(LogLevel.Warn, $"Polly-OnRetry with Exception: {args.Outcome.Exception?.Message}. Backoff Policy retry attempt: {args.AttemptNumber}", args.Outcome.Exception);
                    return default;
                }
            };

            ResiliencePipeline polly = new ResiliencePipelineBuilder()
                .AddRetry(optionsOnRetry)
                .Build();
            #endregion

            _apiProvider = new(polly);

            // PER V3 DOCS: 429	Request has exceeded rate limits. Implementing exponential backoff can help manage retries for failed actions.
            // If you need higher rate limits, reach out to our support team.

            // 1) User-level-limit > Throttles requests by the authenticated user:
            //   5 requests / 1 second

            _callsPerSecond = LookupService.GetLookupValueWithDefault(Constants.REDDIT_API_USER_CALLS_PER_SECOND, 5);
            _fixedWindowInMilliseconds = LookupService.GetLookupValueWithDefault(Constants.REDDIT_API_USER_CALLS_FIXED_WINDOW_IN_MILLISECONDS, 1000);
        }

        public void Execute()
        {
            if (_queueItems.Count == 0)
            {
                _log(LogLevel.Info, "There are no reports in the Queue");
                return;
            }

            _runtime.Start();

            foreach (var queueItem in _queueItems)
            {
                try
                {
                    if (IsMaxRunTimeReached())
                    {
                        break;
                    }

                    if (_invalidAccountIds.Contains(queueItem.EntityID))
                    {
                        _log(LogLevel.Info, $"{queueItem.FileGUID}-{queueItem.FileDate}-ServerID {queueItem.EntityID} has been flagged as error earlier in this Import job and will be skipped.");
                        continue;
                    }

                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                    // let's clean any old data in the S3 raw folder
                    DeleteRawFiles(queueItem);

                    List<ApiReportItem> reportList = new();

                    foreach (var apiReport in _apiReports.OrderBy(a => a.ReportSettings.Order))
                    {
                        var apiReportType = apiReport.ReportSettings.ReportType;

                        //The dimension data for an entity is the same regardless of file date.
                        //If we've already downloaded the reports, then we can skip adding it to the list of reports we need to download
                        if (apiReportType == REPORT_TYPE_DIMENSION && _downloadedDimReports.Contains(queueItem.EntityID))
                        {
                            continue;
                        }

                        var reportItem = new ApiReportItem
                        {
                            ReportName = apiReport.APIReportName.ToLower(),
                            QueueID = queueItem.ID,
                            FileGuid = queueItem.FileGUID,
                            ReportType = apiReportType,
                            APIReport = apiReport,
                            FileDate = queueItem.FileDate,
                            AccountID = queueItem.EntityID
                        };

                        reportList.Add(reportItem);

                        // Get the account report first in order to get the time zone ID
                        // which will be used in retrieving metrics reports
                        if (apiReportType == REPORT_TYPE_DIMENSION && apiReport.APIReportName == ACCOUNT_REPORT_NAME)
                        {
                            DownloadReports(reportList, queueItem);
                        }
                    }

                    DownloadReports(reportList, queueItem);

                    AreAllReportsDownloaded(reportList, queueItem);

                    CreateManifestFiles(reportList, queueItem);
                }
                catch (AggregateException ae)
                {
                    foreach (var ex in ae.InnerExceptions)
                    {
                        _logEx(LogLevel.Error, $"Aggregate-InnerException-Error Failed to download reports for queue -> Failed for FileGUID: {queueItem.FileGUID}- Exception: {ex.Message} - STACK {ex.StackTrace}", ex);
                        _exceptionCount++;
                    }
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    _invalidAccountIds.Add(queueItem.EntityID);
                }
                catch (Exception ex)
                {
                    _logEx(LogLevel.Error, $"Failed to download reports for queue -> Failed for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}. Message:{ex.Message} - STACK {ex.StackTrace}", ex);
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    _exceptionCount++;
                    _invalidAccountIds.Add(queueItem.EntityID);
                }
            }

            _runtime.Stop();
            _log(LogLevel.Info, $"Import job complete. Run Time: {_runtime.Elapsed}");

            if (_exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
            }
            else if (_warningCount > 0)
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
            }
        }

        private void DownloadReports(List<ApiReportItem> reportList, Queue queueItem)
        {
            List<ApiRequest> requestList = new();
            reportList.Where(x => !x.IsDownloaded).ToList().ForEach(report => requestList.Add(GetRequest(report, queueItem)));

            ParallelOptions parallelOptions = new() { MaxDegreeOfParallelism = _maxDegreeOfParallelism };
            _apiProvider.MakeParallelCallsWithPaging(new ParallelApiCallerArguments<ApiRequest, string>()
            {
                PermittedItems = _callsPerSecond,
                WindowInMilliseconds = _fixedWindowInMilliseconds,
                ParallelOptions = parallelOptions,
                RequestItems = requestList,
                PagingFunction = (apiRequest, response) =>
                {
                    if (!string.IsNullOrEmpty(apiRequest.ApiReportItem.APIReport.ReportSettings.CacheKeyValue))
                    {
                        string[] keyValuePath = apiRequest.ApiReportItem.APIReport.ReportSettings.CacheKeyValue.Split('|');
                        string keyPrefix = keyValuePath[0];
                        string valuePath = keyValuePath[1];

                        string key = $"{keyPrefix}_{queueItem.EntityID.ToLower()}";
                        string value = JObject.Parse(response).SelectToken(valuePath)?.Value<string>();

                        _dimensionValuesCache.TryAdd(key, value);
                    }

                    apiRequest.NextPageUrl = JObject.Parse(response).SelectToken("pagination.next_url")?.Value<string>();

                    if (apiRequest.NextPageUrl == null)
                    {
                        return null;
                    }

                    apiRequest.ApiReportItem.CurrentPageIndex++;
                    return apiRequest;
                },
                CurrentItemAction = (apiRequest) =>
                {
                    apiRequest.ApiReportItem.IsDownloaded = true;
                    _log(LogLevel.Info, $"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{apiRequest.ApiReportItem.APIReport.APIReportName}");
                },
                StreamResultAction = (stream, apiRequest) =>
                {
                    apiRequest.ApiReportItem.FileName = $"{apiRequest.ApiReportItem.ReportName}_{queueItem.FileGUID}_{apiRequest.ApiReportItem.CurrentPageIndex}.{apiRequest.ApiReportItem.APIReport.ReportSettings.FileExtension}";
                    string[] paths = new string[]
                    {
                        queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), apiRequest.ApiReportItem.FileName
                    };

                    S3File file = new(RemoteUri.CombineUri(_baseRawDestUri, paths), GreenhouseS3Creds);

                    var incomingFile = new StreamFile(stream, GreenhouseS3Creds);

                    _log(LogLevel.Info, $"{queueItem.FileGUID}->{apiRequest.ApiReportItem.APIReport.APIReportName} - Saving to S3 as {apiRequest.ApiReportItem.FileName}");

                    UploadToS3(incomingFile, file, paths);

                    apiRequest.ApiReportItem.FileCollectionList.Add(new FileCollectionItem()
                    {
                        FileSize = file.Length,
                        SourceFileName = apiRequest.ApiReportItem.APIReport.APIReportName,
                        FilePath = $"{_baseRawDestUri.ToString().TrimStart('/')}/{queueItem.EntityID.ToLower()}/{GetDatedPartition(queueItem.FileDate)}/" + apiRequest.ApiReportItem.FileName
                    });

                    apiRequest.ApiReportItem.FileDate = UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, file.LastWriteTimeUtc);
                },
                ExceptionsAction = (apiRequest, ex) =>
                {
                    _logEx(LogLevel.Error, $"Failed to download report -> failed on: {apiRequest.ApiReportItem.FileGuid} " +
                                              $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Report:{apiRequest.ApiReportItem.ReportName} ->" +
                                              $"Error -> Exception: {ex.Message} -> STACK {ex.StackTrace}", ex);
                    apiRequest.ApiReportItem.IsDownloaded = false;
                },
                MaxRunTimeReachedFunction = () => IsMaxRunTimeReached()
            });
        }

        private bool IsMaxRunTimeReached()
        {
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
            {
                _log(LogLevel.Warn, $"Maximum runtime reached. -> Maximum Runtime: {_maxRuntime} -> Elapsed Runtime (in mins): {_runtime.Elapsed.TotalMinutes}");
                _warningCount++;
                return true;
            }

            return false;
        }

        private ApiRequest GetRequest(ApiReportItem apiReportItem, Queue queueItem)
        {
            ApiRequest apiRequest = new(HttpClientProvider, $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{apiReportItem.APIReport.ReportSettings.Path.TrimEnd('/')}"
                , _oAuth, apiReportItem.APIReport.ReportSettings.DeliveryPath)
            {
                AccountID = apiReportItem.AccountID,
                ApiReportItem = apiReportItem
            };

            if (apiReportItem.ReportType == REPORT_TYPE_METRICS)
            {
                apiRequest.MethodType = HttpMethod.Post;

                if (!_dimensionValuesCache.TryGetValue(string.Format(null, TIMEZONE_KEY, queueItem.EntityID.ToLower()), out string timeZoneID))
                {
                    // if the cache does not contain the timezone for that entity, we fail the call
                    // because making the call without it would result in bad data
                    throw new APIReportException(_timezoneMissingException);
                }

                var timeZoneInfo = TZConvert.GetTimeZoneInfo(timeZoneID);
                var dataDate = TimeZoneInfo.ConvertTimeToUtc(queueItem.FileDate, timeZoneInfo);
                var formattedDataDate = dataDate.ToString("yyyy-MM-ddTHH:mm:00Z");

                apiRequest.BodyRequest = new MetricsRequestBody
                {
                    Data = new()
                    {
                        Breakdowns = apiReportItem.APIReport.ReportSettings.GroupBy.Split(","),
                        Fields = apiReportItem.APIReport.ReportFields.Select(r => r.APIReportFieldName).ToArray(),
                        StartsAt = formattedDataDate,
                        EndsAt = formattedDataDate,
                        TimeZoneID = timeZoneID
                    }
                };
                apiRequest.SetParameters(_pageSize);
            }
            else if (apiReportItem.ReportType == REPORT_TYPE_DIMENSION)
            {
                apiRequest.MethodType = HttpMethod.Get;
            }

            return apiRequest;
        }

        private void CreateManifestFiles(List<ApiReportItem> reportList, Queue queueItem)
        {
            var allReports = reportList.Where(q => q.FileCollectionList != null).SelectMany(q => q.FileCollectionList).ToList();

            var dimensionReports = reportList.Where(x => x.ReportType == REPORT_TYPE_DIMENSION).SelectMany(x => x.FileCollectionList).ToList();

            // check if dimension reports were requested for this queue
            // if so, cache them for the next queue with same entityid
            // otherwise get them from cache
            if (dimensionReports.Count != 0)
            {
                //Add Dimension reports to the raw manifest files for this api entity
                _dimensionCache.Add(queueItem.EntityID, dimensionReports);
            }
            else
            {
                //Get the existing manifest files for the entity and add it to the list
                if (!_dimensionCache.TryGetValue(queueItem.EntityID, out dimensionReports))
                {
                    //If the dimension reports for the Entity ID were not found, then there was a critical issue in adding it to the dictionary.
                    throw new APIReportException($"Critical error. Failed to find manifest files for {queueItem.EntityID}  -> FileGUID: {queueItem.FileGUID}");
                }

                allReports.AddRange(dimensionReports);
            }

            var manifestFiles = DAL.ETLProvider.CreateManifestFiles(queueItem, allReports, _baseRawDestUri, GetDatedPartition);

            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(manifestFiles);
            queueItem.FileSize = allReports.Sum(x => x.FileSize);
            queueItem.DeliveryFileDate = reportList.Max(x => x.FileDate);

            queueItem.Status = Constants.JobStatus.Complete.ToString();
            queueItem.StatusId = (int)Constants.JobStatus.Complete;
            JobService.Update(queueItem);
        }

        private void AreAllReportsDownloaded(List<ApiReportItem> reportList, Queue queueItem)
        {
            if (!reportList.TrueForAll(r => r.IsDownloaded))
            {
                throw new APIReportException("Not all reports downloaded - marking queue as Error");
            }

            // all the reports were downloaded, if any dimension report let s keep track of the entityID to not request it again
            if (reportList.Exists(x => x.ReportType == REPORT_TYPE_DIMENSION))
            {
                _downloadedDimReports.Add(queueItem.EntityID);
            }
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

        public void PostExecute() { }
    }
}
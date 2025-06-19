using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Pinterest;
using Greenhouse.Data.DataSource.Pinterest;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.Pinterest;

[Export("Pinterest-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = LogManager.GetCurrentClassLogger();
    private ApiClient _apiClient;

    private RemoteAccessClient remoteAccessClient;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private List<IFileItem> queueItems;
    private IEnumerable<APIReport<ReportSettings>> _reports;
    private JsonSerializerSettings redshiftSerializerSettings;
    private List<string> pinterestReportingJobRetryCodeList;
    private string callsRemainingHeader;
    private string resetSecondsHeader;
    private string rateLimitHeader;
    private int totalDaysValid;
    private List<long> queueIdList;
    private DimensionState dimensionState;
    private int nbIDsPerCall;
    private int dimPageSize;
    private int _maxRetry;
    private string JobGUID => base.JED.JobGUID.ToString();
    private Action<string> LogInfo;
    private List<APIEntity> _apiEntities;
    private PinterestOAuth oAuth;
    private string dimStateLookupValue;
    private int exceptionCount;
    private List<string> _locationRegionOptIns;
    private List<string> _appTypeOptIns;

    public void PreExecute()
    {

        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        baseRawDestUri = GetDestinationFolder();
        LogInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));

        oAuth = new PinterestOAuth(HttpClientProvider, CurrentCredential);

        LogInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        _apiEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID).ToList();

        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)?.ToList();

        remoteAccessClient = base.GetS3RemoteAccessClient();
        _reports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        redshiftSerializerSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.None
        };
        redshiftSerializerSettings.Converters.Add(new Utilities.IO.RedshiftConverter());
        _apiClient = new ApiClient(HttpClientProvider, oAuth, CurrentIntegration);
        InitFromLookup();
    }

    private void InitFromLookup()
    {
        var pinterestReportingJobRetryCode = SetupService.GetById<Lookup>(Constants.PINTEREST_REPORTING_JOB_RETRY_CODE);
        pinterestReportingJobRetryCodeList = string.IsNullOrEmpty(pinterestReportingJobRetryCode?.Value) ? new List<string>() : (pinterestReportingJobRetryCode.Value).Split(',').ToList();

        var callsRemainingHeaderLookup = SetupService.GetById<Lookup>(Constants.PINTEREST_HEADER_REMAINING_CALLS);
        callsRemainingHeader = string.IsNullOrEmpty(callsRemainingHeaderLookup?.Value) ? "RateLimit-Remaining" : callsRemainingHeaderLookup.Value;

        var resetSecondsHeaderLookup = SetupService.GetById<Lookup>(Constants.PINTEREST_HEADER_RESET_SECONDS);
        resetSecondsHeader = string.IsNullOrEmpty(resetSecondsHeaderLookup?.Value) ? "Retry-After" : resetSecondsHeaderLookup.Value;

        var rateLimitHeaderLookup = SetupService.GetById<Lookup>(Constants.PINTEREST_HEADER_RATE_LIMIT_NAME);
        rateLimitHeader = string.IsNullOrEmpty(rateLimitHeaderLookup?.Value) ? "RateLimit-Limit" : rateLimitHeaderLookup.Value;

        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.PINTEREST_ASYNC_TASK_TOTAL_DAYS_VALID)?.Value, out totalDaysValid))
        {
            totalDaysValid = 30;
        }

        queueIdList = JobService.GetQueueIDBySource(base.SourceId, CurrentIntegration.IntegrationID)?.ToList();

        dimStateLookupValue = Constants.PINTEREST_DIM_STATE + $"_{CurrentIntegration.IntegrationID}";
        var dimensionStateLookup = SetupService.GetById<Lookup>(dimStateLookupValue);
        dimensionState = string.IsNullOrEmpty(dimensionStateLookup?.Value) ? new DimensionState() : ETLProvider.DeserializeType<DimensionState>(dimensionStateLookup.Value);

        var nbIDsPerCallLookup = SetupService.GetById<Lookup>(Constants.PINTEREST_NUM_ID_PER_DIM_CALL);
        if (!int.TryParse(nbIDsPerCallLookup?.Value, out nbIDsPerCall))
        {
            nbIDsPerCall = 100;
        }

        var dimPageSizeLookup = SetupService.GetById<Lookup>(Constants.PINTEREST_DIM_PAGE_SIZE);
        if (!int.TryParse(dimPageSizeLookup?.Value, out dimPageSize))
        {
            dimPageSize = 100;
        }

        _appTypeOptIns = ETLProvider.DeserializeType<List<string>>(SetupService.GetById<Lookup>(Constants.PINTEREST_DELIVERYAPPTYPE_REPORT_OPT_IN).Value);
        _locationRegionOptIns = ETLProvider.DeserializeType<List<string>>(SetupService.GetById<Lookup>(Constants.PINTEREST_DELIVERYLOCATIONREGION_REPORT_OPT_IN).Value);
    }

    public void Execute()
    {
        exceptionCount = 0;

        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.PINTEREST_POLLY_MAX_RETRY).Value, out _maxRetry))
            _maxRetry = 10;

        var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
        {
            Counter = 3,
            MaxRetry = _maxRetry
        };

        List<ApiReportItem> reportList = new List<ApiReportItem>();
        var lookupReports = new List<ApiReportItem>();

        var lookupString = Constants.PINTEREST_UNFINISHED_REPORTS + $"_{CurrentIntegration.IntegrationID}";
        var lookup = JobService.GetById<Lookup>(lookupString);
        if (!string.IsNullOrEmpty(lookup?.Value))
        {
            lookupReports = GetLookupReports(lookup);
        }
        else
        {
            lookup = new Lookup { Name = lookupString };
        }

        if (queueItems.Count != 0)
        {
            //move unfinished reports to top of the report list 
            if (lookupReports.Count != 0)
                reportList.AddRange(lookupReports);

            QueueMetricReports(apiCallsBackOffStrategy, reportList);
            DownloadReports(apiCallsBackOffStrategy, reportList);

            // saving dimension state
            SaveState();

            HandleErrors(reportList, apiCallsBackOffStrategy, lookupReports, lookup);
        }//end if queue.Any()
        else
        {
            LogInfo("There are no reports in the Queue");
        }

        LogInfo("Import job complete");
    }

    private void HandleErrors(List<ApiReportItem> reportList, ExponentialBackOffStrategy apiCallsBackOffStrategy,
        List<ApiReportItem> lookupReports, Lookup lookup)
    {
        var unfinishedReports = reportList.Where(x => !x.IsReady);

        if (unfinishedReports.Any())
        {
            string errMessage = PrefixJobGuid(
                $"There are unfinished reports after max retry [{apiCallsBackOffStrategy.MaxRetry}] has been reached. There are {exceptionCount} error counts.  Updating queue status to 'Error': {JsonConvert.SerializeObject(unfinishedReports)}");
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, errMessage));
            var unfinishedReportGroup = unfinishedReports
                .GroupBy(x => x.QueueID)
                .Where(g => g.All(r => r.Status != ReportStatus.FAILED.ToString()));

            var unfinishedQueues = unfinishedReportGroup.Select(x => new Queue { ID = x.Key });

            base.UpdateQueueWithDelete(unfinishedQueues, Common.Constants.JobStatus.Pending, false);
            //clear previous unfinished reports
            lookupReports.Clear();

            //Get all reports for queue that do not have any failed associated reports. Will store in Lookup table for next job run
            //Need all reports because FileCollection is not updated until all reports are readya and have been saved.
            var reports = reportList.Where(r => unfinishedQueues.Any(q => q.ID == r.QueueID));

            lookupReports.AddRange(reports);
            lookup.Value = JsonConvert.SerializeObject(lookupReports);
            lookup.LastUpdated = DateTime.Now;

            Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
            Data.Repositories.LookupRepository.AddOrUpdateLookup(lookup);

            LogInfo($"Stored unfinished reports in Lookup table");

            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = "Unfinished reports. Some reports were not ready during this Import and will be picked up by the next one.";
        }
        else //Clear existing Lookup value
        {
            lookup.Value = string.Empty;
            lookup.LastUpdated = DateTime.Now;
            JobService.Update(lookup);
            LogInfo($"No errors. Cleared lookup valued");
        }

        if (exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {exceptionCount}; Please check Splunk for more detail.");
        }
    }

    private sealed class DimensionState
    {
        public DimensionState()
        {
            StatusDate = DateTime.UtcNow;
            IDsPerReportType = new Dictionary<string, List<string>>();
        }

        public DateTime StatusDate { get; set; }

        public Dictionary<string, List<string>> IDsPerReportType { get; set; }
    }

    private void DownloadReports(IBackOffStrategy apiCallsBackOffStrategy, List<ApiReportItem> reportList)
    {
        var cachedIDs = new Dictionary<string, List<string>>();

        if (reportList.Count != 0)
        {
            bool allDone = false;
            var policy = base.GetPollyRetryPolicy<bool>(this.JobGUID, apiCallsBackOffStrategy, (bool allReportsDone) => allReportsDone == false);

            policy.Execute(() =>
            {
                var reports = reportList.Where(x => !x.IsReady).ToList();
                var reportCount = reports.Count;
                for (int i = 0; i < reportCount; i++)
                {
                    var reportItem = reports[i];
                    try
                    {
                        //check if an associated report for the same queue item has failed, and skip it if it has.
                        if (reports.Any(x => x.QueueID == reportItem.QueueID && x.Status == Data.DataSource.Pinterest.ReportStatus.FAILED.ToString()))
                        {
                            reportItem.IsReady = true;
                            reportItem.Status = Data.DataSource.Pinterest.ReportStatus.FAILED.ToString();
                            LogInfo($"Skipping Check Report Status: FileGUID: {reportItem.FileGuid}->Report Name: {reportItem.Report.APIReportName} because associated report failed");
                            continue;
                        }

                        var queueItem = queueItems.Find(x => x.ID == reportItem.QueueID) as Queue;
                        var apiReport = CheckStatus(reportItem);

                        // if this report doesnt have any data, we move to the next one
                        if (apiReport == null)
                        {
                            reportItem.IsReady = true;
                            reportItem.IsDownloaded = true;
                            continue;
                        }

                        reportItem.Status = apiReport.ReportStatus;

                        if (reportItem.Status == ReportStatus.FINISHED.ToString())
                        {
                            reportItem.IsReady = true;
                            DownloadAsyncReport(reportItem, queueItem, apiReport, cachedIDs);
                        }
                        else if (reportItem.Status == ReportStatus.FAILED.ToString()) /* Error out Queue */
                        {
                            reportItem.IsReady = true;
                            queueItem.StatusId = (int)Constants.JobStatus.Error;
                            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                            continue;
                        }

                        var queueReportList = reportList.Where(x => x.QueueID == queueItem.ID);
                        bool done = queueReportList.All(x => x.IsDownloaded == true && x.IsReady == true);
                        if (done)
                        {
                            LogInfo($"All reports of the queue with Fileguid '{queueItem.FileGUID}' are ready");

                            var dimReports = DownloadDimensionsReports(queueItem, cachedIDs);

                            //stage files from raw json
                            var rawMetricFiles = queueReportList.Where(q => q.FileCollection != null).Select(q => q.FileCollection).ToList();

                            var stagedFiles = StageReports(queueItem, rawMetricFiles.Concat(dimReports).ToList());

                            var manifestFiles = ETLProvider.CreateManifestFiles(queueItem, stagedFiles, baseRawDestUri, GetDatedPartition);

                            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(manifestFiles);
                            queueItem.FileSize = stagedFiles.Sum(x => x.FileSize);
                            queueItem.DeliveryFileDate = queueReportList.Max(x => x.FileDate);

                            queueItem.Status = Constants.JobStatus.Complete.ToString();
                            queueItem.StatusId = (int)Constants.JobStatus.Complete;
                            JobService.Update(queueItem);
                        }
                    }
                    catch (WebException wex)
                    {
                        exceptionCount++;

                        HttpWebResponse httpWebResponse = wex.Response as HttpWebResponse;
                        string errorMessage = string.Empty;

                        if (!(httpWebResponse is null))
                        {
                            using (StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream()))
                            {
                                errorMessage = streamReader.ReadToEnd();
                            }

                            var hasRetryHeader = httpWebResponse.Headers.AllKeys.Contains(resetSecondsHeader);

                            if (httpWebResponse.StatusCode.ToString() == "429" && hasRetryHeader)
                            {
                                var resetSecondsString = httpWebResponse.Headers[resetSecondsHeader];
                                ThreadSleep(resetSecondsString);
                            }
                        }
                        else
                        {
                            errorMessage = "httpWebResponse is null";
                        }

                        var data = JsonConvert.DeserializeObject<MetricReportResponse>(errorMessage);

                        //Sample response if report is still processing:
                        //{ "status": "failure", "code": 1105, "error": { "message": "None"}, "message": "Your job is still processing, please try again later.", "data": null }
                        //Lookup value is a list of codes including 1105
                        if (pinterestReportingJobRetryCodeList.Any(x => x.TrimStart().TrimEnd() == data.Code))
                        {
                            reportItem.Status = ReportStatus.IN_PROGRESS.ToString();
                            reportItem.IsReady = false;
                            LogInfo($"Job is still processing per response code: " +
                                $"FileGUID: {reportItem.FileGuid}->EntityID: {reportItem.ProfileID}->Report Name: {reportItem.Report.APIReportName} because associated report failed" +
                                $"->Error Message: {errorMessage} -> Exception: {wex.Message}");
                        }
                        else
                        {
                            reportItem.Status = ReportStatus.FAILED.ToString();
                            reportItem.IsReady = true;
                            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);

                            logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                                base.PrefixJobGuid($"Web Exception Error checking report status- failed on queueID: {reportItem.QueueID} " +
                                $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.Report.APIReportName} ->Error Message: {errorMessage} -> Exception: {wex.Message} -> StackTrace: {wex.StackTrace}"), wex));
                        }
                    }
                    catch (HttpClientProviderRequestException exc)
                    {
                        HandleException(reportItem, exc);
                    }
                    catch (Exception exc)
                    {
                        HandleException(reportItem, exc);
                    }//end try catch
                } //end for

                allDone = reportList.All(x => x.IsReady == true);
                return allDone;
            });
        }
        else
        {
            LogInfo("There are no reports to run");
        }
    }

    private void HandleException<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        exceptionCount++;
        reportItem.Status = ReportStatus.FAILED.ToString();
        reportItem.IsReady = true;
        JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);

        var logMsg = BuildLogMessage(reportItem, exc);

        logger.Log(Msg.Create(LogLevel.Error, logger.Name,
           base.PrefixJobGuid(logMsg), exc));
    }

    private static string BuildLogMessage<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Error checking report status- failed on queueID: {reportItem.QueueID} " +
           $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.Report.APIReportName} - Exception details: {httpEx}",
            _ =>
                $"Error checking report status- failed on queueID: {reportItem.QueueID} " +
                           $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.Report.APIReportName}  - Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }

    private void DownloadAsyncReport(ApiReportItem reportItem, Queue queueItem, MetricReportResponse apiReport, Dictionary<string, List<string>> cachedIDs)
    {
        var downloadPolicy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxRetry,
            Seed = 1,
            RandomMin = 0,
            RandomMax = 0
        };
        var getDownloadPolicy = base.GetPollyRetryPolicy<bool>(queueItem.FileGUID.ToString(), downloadPolicy, (bool downloadComplete)
            => downloadComplete == false);

        S3File file = null;
        reportItem.IsDownloaded = getDownloadPolicy.Execute((_) => DownloadReportFile(reportItem, out file),
            new Dictionary<string, object> { { "methodName", "DownloadReport" } });

        if (reportItem.IsDownloaded)
        {
            // if another report needs some of the data from this report, we cache it
            // for example the campaign dimension report will need a list of all the campaign ids

            GetDataForDimReports(reportItem, queueItem, cachedIDs, file);
        }
        else
        {
            //Error queue Item, if any of its report types failed to download
            reportItem.Status = Data.DataSource.Pinterest.ReportStatus.FAILED.ToString();
            queueItem.StatusId = (int)Constants.JobStatus.Error;
            JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
        }
    }

    private void GetDataForDimReports(ApiReportItem reportItem, IFileItem queueItem, Dictionary<string, List<string>> cachedIDs, S3File file)
    {
        // get the list of reports dependent of the data from this reportItem
        var dimReports = _reports.Where(r => r.IsActive &&
                                             r.ReportSettings.GetIDsFrom != null &&
                                             r.ReportSettings.GetIDsFrom.ReportName ==
                                             reportItem.Report.APIReportName);

        foreach (var dimReport in dimReports)
        {
            // retrieving the data from the report using the json path  value stored in dimReport.ReportSettings.GetIDsFrom.PathDoData

            using (var reader = new StreamReader(file.Get()))
            {
                string content = reader.ReadToEnd();
                JObject o = JObject.Parse(content);
                var ids = o.SelectTokens(dimReport.ReportSettings.GetIDsFrom.PathDoData)
                    .Select(v => v.ToString());

                LogInfo(
                    $"{ids.Count()} extracted values for {dimReport.APIReportName} from report {reportItem.Report.APIReportName}");

                var key = GenerateCacheKey(queueItem, dimReport);
                if (!cachedIDs.TryGetValue(key, out List<string> value))
                {
                    value = new List<string>();
                    cachedIDs.Add(key, value);
                }
                var values = value;
                values.AddRange(ids);
                cachedIDs[key] = values.Distinct().ToList();
            }
        }
    }

    private static string GenerateCacheKey(IFileItem queueItem, APIReport<ReportSettings> dimReport)
    {
        return $"{queueItem.ID}_{dimReport.ReportSettings.GetIDsFrom.CacheKey}";
    }

    private List<FileCollectionItem> DownloadDimensionsReports(IFileItem queueItem, Dictionary<string, List<string>> cachedIDs)
    {
        var dimReports = new List<FileCollectionItem>();
        var today = DateTime.UtcNow.Date;

        // dimensions are downloaded only once a day
        // state is reset if expired
        if (dimensionState.StatusDate.Date < today)
        {
            dimensionState = new DimensionState();
        }

        var dimensionReports = _reports.Where(r => r.ReportSettings.ReportType == "dimensions");

        foreach (var report in dimensionReports)
        {
            // first lets retrieve the list of ids to retrieve dimensions for
            var IDsToRetrieve = GetIDsToRetrieve(queueItem, cachedIDs, report);

            // we retrieve dim data once a day, we retrieve the list of the ids already requested Today
            var IDsAlreadyRequested = GetIDsAlreadyRequested(report, out var stateKey);

            // final list to retrieve
            var IDs = IDsToRetrieve.Except(IDsAlreadyRequested).ToList();

            if (IDs.Count == 0) continue;

            switch (report.ReportSettings.QueryType)
            {
                // case to retrieve once id in 1 call
                case "single":
                    LogInfo($"Report {report.APIReportName}, {IDs.Count} entities to request - single mode");

                    foreach (var id in IDs)
                    {
                        var url = string.Format(report.ReportSettings.DeliveryPath, id);
                        var parameters = new Dictionary<string, string> { { "page_size", dimPageSize.ToString() } };
                        var files = DownloadDimFiles(report, queueItem, url, parameters);
                        dimReports.AddRange(files);
                    }

                    break;
                // case to retrieve multiple ids in 1 call
                case "multiple":
                    LogInfo($"Report {report.APIReportName}, {IDs.Count} entities to request - multiple mode");

                    var values = IDs;
                    int chunkNumber = 0;

                    // retrieving nbIDsPerCall entities per call
                    // creating chunks of nbIDsPerCall ids
                    // different from dimPageSize that specifies how many results returned
                    while (values.Count != 0)
                    {
                        var valuesChunk = values.Take(nbIDsPerCall);
                        var url =
                            $"{report.ReportSettings.DeliveryPath}";
                        var parameters = new Dictionary<string, string>
                        {
                            { "page_size", dimPageSize.ToString() },
                            { report.ReportSettings.QueryString, string.Join(",", valuesChunk)},
                            { "entity_statuses", report.ReportSettings.Entitystatuses }
                        };
                        var files = DownloadDimFiles(report, queueItem, url, parameters, chunkNumber);
                        dimReports.AddRange(files);

                        values = values.Skip(nbIDsPerCall).ToList();
                        chunkNumber++;
                    }
                    break;
                default:
                    throw new APIReportException($"Unsupported queryType:{report.ReportSettings.QueryType}. Allowed values are single and multiple");
            }

            // update dim state with the ids retrieved
            if (!dimensionState.IDsPerReportType.TryGetValue(stateKey, out List<string> value))
            {
                value = new List<string>();
                dimensionState.IDsPerReportType.Add(stateKey, value);
            }

            var ids = value;
            ids.AddRange(IDsToRetrieve);
            dimensionState.IDsPerReportType[stateKey] = ids.Distinct().ToList();
        }

        return dimReports;
    }

    private List<string> GetIDsAlreadyRequested(APIReport<ReportSettings> report, out string stateKey)
    {
        //removing any id that was already requested Today
        var IDsAlreadyRequested = new List<string>();

        // the state keeps track of the ids requested at the report level
        // as an id retrieved for a queue
        stateKey = report.ReportSettings.GetIDsFrom.CacheKey;

        if (dimensionState.IDsPerReportType.TryGetValue(stateKey, out List<string> value))
        {
            IDsAlreadyRequested = value;
        }

        return IDsAlreadyRequested;
    }

    private static List<string> GetIDsToRetrieve(IFileItem queueItem, Dictionary<string, List<string>> cachedIDs, APIReport<ReportSettings> report)
    {
        var key = GenerateCacheKey(queueItem, report);
        if (!cachedIDs.TryGetValue(key, out List<string> value)) return new List<string>();
        var IDsToRequest = value;
        return IDsToRequest;
    }

    private List<FileCollectionItem> DownloadDimFiles(APIReport<ReportSettings> report, IFileItem queueItem, string urlPath, Dictionary<string, string> parameters, int? chunkNumber = null)
    {
        var fileCollection = new List<FileCollectionItem>();

        var downloadPolicy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxRetry,
            Seed = 1,
            RandomMin = 0,
            RandomMax = 0
        };

        var getDownloadPolicy = base.GetPollyPolicy<Exception>(queueItem.FileGUID.ToString(), downloadPolicy);

        getDownloadPolicy.Execute((_) =>
            {
                fileCollection = SendDimRequest(report, queueItem, urlPath, parameters, chunkNumber);
            },
            new Dictionary<string, object> { { "methodName", "DownloadDimFiles" } });

        return fileCollection;
    }

    private List<FileCollectionItem> SendDimRequest(APIReport<ReportSettings> report, IFileItem queueItem, string urlPath, Dictionary<string, string> parameters, int? chunkNumber = null)
    {
        List<FileCollectionItem> fileCollection = new List<FileCollectionItem>();

        // object in charge of building the request and retrieving the results
        var request = new DimRequest(_apiClient, report, queueItem, urlPath, chunkNumber, parameters, CurrentIntegration);

        int page = 0;
        string nextPageTag = null;

        // loop through pages of results
        do
        {
            var fileName = request.GetFileName(page);
            LogInfo(
                    $"{CurrentSource.SourceName} start DownloadDimFiles: queueID: {queueItem.ID}->{report.APIReportName}. Saving to S3 as {fileName}");

            var apiReport = request.GetReponse(nextPageTag);
            CheckRateLimit(apiReport);
            var encoding = new UTF8Encoding(false);

            using (var stream = new MemoryStream(encoding.GetBytes($"{{\"items\":{apiReport.Items}}}")))
            {
                string[] paths = new string[]
                {
                    queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName
                };

                var filePathUri = RemoteUri.CombineUri(this.baseRawDestUri, paths);
                var file = new S3File(filePathUri, GreenhouseS3Creds);

                var incomingFile = new StreamFile(stream, GreenhouseS3Creds);
                base.UploadToS3(incomingFile, file, paths);

                fileCollection.Add(new FileCollectionItem
                {
                    FileSize = file.Length,
                    SourceFileName = report.APIReportName,
                    FilePath = filePathUri.AbsoluteUri
                });
            }

            // pinterest paging: returns a tag to provide as a query string in the following call
            nextPageTag = apiReport.NextPageTag;

            page++;
        } while (!string.IsNullOrEmpty(nextPageTag));

        return fileCollection;
    }

    // Contains the logic to build the request
    private sealed class DimRequest
    {
        private readonly APIReport<ReportSettings> report;
        private readonly IFileItem queueItem;
        private readonly string urlPath;
        private readonly int? chunkNumber;
        private readonly Dictionary<string, string> parameters;
        private readonly ApiClient _apiClient;

        public DimRequest(ApiClient apiclient, APIReport<ReportSettings> report, IFileItem queueItem, string urlPath, int? chunkNumber, Dictionary<string, string> parameters, Integration integration)
        {
            this.report = report;
            this.queueItem = queueItem;
            this.urlPath = urlPath;
            this.chunkNumber = chunkNumber;
            this.parameters = parameters;
            _apiClient = apiclient;

        }

        public string GetFileName(int page)
        {
            string pageString = chunkNumber.HasValue ? $"_{chunkNumber}" : "";
            return
                $"{report.APIReportName}_{queueItem.FileGUID}{pageString}_{page}.{report.ReportSettings.FileExtension}";
        }

        public DimReportResponse GetReponse(string nextPageTag)
        {
            string nextPageParamName = "bookmark";

            if (!string.IsNullOrEmpty(nextPageTag))
            {
                parameters.TryAdd(nextPageParamName, string.Empty);
                parameters[nextPageParamName] = nextPageTag;
            }

            string queryString = "";

            if (parameters.Count != 0)
            {
                queryString = "?";

                foreach (var param in parameters)
                {
                    queryString += $"{param.Key}={param.Value}&";
                }
            }

            var options = new RequestApiReportOptions()
            {
                UrlExtension = $"{report.ReportSettings.Path.TrimEnd('/')}/{queueItem.EntityID}/{urlPath}{queryString}",
                MethodType = HttpMethod.Get,
            };

            var apiReport = _apiClient.RequestApiReportAsync<DimReportResponse>(options).GetAwaiter().GetResult();
            return apiReport;
        }
    }

    private MetricReportResponse CheckStatus(ApiReportItem reportItem)
    {
        var apiReport = new MetricReportResponse();

        //Empty reports
        if (reportItem.IsDownloaded && reportItem.ReportToken == "0")
        {
            return null;
        }

        var reportType = _reports.FirstOrDefault(x => x.APIReportName.Equals(reportItem.Report.APIReportName, StringComparison.InvariantCultureIgnoreCase));

        var deliveryPath = reportType?.ReportSettings.DeliveryPath ?? "delivery_metrics/";
        var options = new RequestApiReportOptions()
        {
            UrlExtension = $"{reportType?.ReportSettings.Path.TrimEnd('/')}/{reportItem.ProfileID}/{deliveryPath}?token={reportItem.ReportToken}",
            MethodType = HttpMethod.Get
        };

        apiReport = _apiClient.RequestApiReportAsync<MetricReportResponse>(options).GetAwaiter().GetResult();
        CheckRateLimit(apiReport);
        LogInfo($"Check Report Status: FileGUID: {reportItem.FileGuid}->API Response: {JsonConvert.SerializeObject(apiReport)}");
        return apiReport;
    }

    private bool DownloadReportFile(ApiReportItem reportItem, out S3File file)
    {
        bool returnVal = false;
        file = null;
        try
        {

            var queueItem = queueItems.Find(q => q.ID == reportItem.QueueID);

            //For v5, we will need to obtain the report URL by using a GET Request on the Reports Endpoint
            var optionsUrl = new DownloadReportOptions()
            {
                UriPath = reportItem.ReportURL + $"?token={reportItem.ReportToken}",
            };

            var reportResponse = _apiClient.GetReportDownloadUrl<MetricReportResponse>(optionsUrl).GetAwaiter().GetResult();

            var options = new DownloadReportOptions()
            {
                UriPath = reportResponse.URL,
            };

            var response = _apiClient.DownloadReportAsync(options).GetAwaiter().GetResult();

            reportItem.FileName = $"{reportItem.Report.APIReportName}_{queueItem.FileGUID}.{reportItem.FileExtension}";
            LogInfo($"{CurrentSource.SourceName} start DownloadReport: queueID: {queueItem.ID}->{reportItem.ReportToken}->{reportItem.Report.APIReportName}->{reportItem.ReportURL}. Saving to S3 as {reportItem.FileName}");

            string[] paths = new string[]
            {
                      queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), reportItem.FileName
            };

            file = new S3File(RemoteUri.CombineUri(this.baseRawDestUri, paths), GreenhouseS3Creds);

            var incomingFile = new StreamFile(response, GreenhouseS3Creds);
            base.UploadToS3(incomingFile, file, paths);

            reportItem.FileCollection = new FileCollectionItem()
            {
                FileSize = file.Length,
                SourceFileName = reportItem.Report.APIReportName,
                FilePath = reportItem.FileName
            };
            reportItem.FileDate = UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, file.LastWriteTimeUtc);

            response.Dispose();

            returnVal = true;

            LogInfo($"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{reportItem.ReportToken}->{reportItem.Report.APIReportName}->{reportItem.ReportURL}. Saving to S3 as {reportItem.FileName}");
        }
        catch (HttpClientProviderRequestException hex)
        {
            returnVal = false;
            HandleExceptionWhenDownloadingReport(reportItem, hex);
        }
        catch (Exception exc)
        {
            returnVal = false;
            HandleExceptionWhenDownloadingReport(reportItem, exc);
        }
        return returnVal;
    }

    private void HandleExceptionWhenDownloadingReport<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        exceptionCount++;
        JobService.UpdateQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);

        var logMsg = BuildLogMessageWhenDownloadingReport(reportItem, exc);

        logger.Log(Msg.Create(LogLevel.Error, logger.Name, base.PrefixJobGuid(logMsg), exc));
    }

    private static string BuildLogMessageWhenDownloadingReport<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"HttpRequestException Error downloading report - failed on queueID: {reportItem.QueueID} " +
                    $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.Report.APIReportName} -> " +
                    $"|Exception details : {httpEx}",
            _ =>
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} " +
                    $" ReportToken: {reportItem.ReportToken} Report Name: {reportItem.Report.APIReportName}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }

    private List<FileCollectionItem> StageReports(Queue queueItem, List<FileCollectionItem> rawFiles)
    {
        var stagedFiles = new List<FileCollectionItem>();
        try
        {
            if (rawFiles.Count == 0)
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    PrefixJobGuid(
                        $"File Collection is empty; unable to stage data for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} ")));
            }
            else
            {
                Func<JArray, string, DateTime, string, FileCollectionItem> writeToFileSignature = ((a, b, c, d) => WriteObjectToFile(a, b, c, d));

                var entity = _apiEntities.FirstOrDefault(x => x.APIEntityCode == queueItem.EntityID);
                Dictionary<string, int> reportCount = new Dictionary<string, int>();

                foreach (var report in rawFiles)
                {
                    if (!reportCount.ContainsKey(report.SourceFileName))
                    {
                        reportCount[report.SourceFileName] = 0; // Default value is 0
                    }
                    else
                    {
                        reportCount[report.SourceFileName] += 1;
                    }

                    var fileName = $"{report.SourceFileName}_{queueItem.FileGUID}_{queueItem.FileDate:yyyy-MM-dd}_{reportCount[report.SourceFileName]}.json";

                    var file = StageReport(queueItem, report, writeToFileSignature, entity, fileName);
                    stagedFiles.Add(file);
                }
            }
        }
        catch (HttpClientProviderRequestException exc)
        {
            HandleExceptionWhenStagingData(queueItem, exc);
        }
        catch (Exception exc)
        {
            HandleExceptionWhenStagingData(queueItem, exc);
        }

        return stagedFiles;
    }
    private void HandleExceptionWhenStagingData<TException>(Queue queueItem, TException exc) where TException : Exception
    {
        exceptionCount++;
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        queueItem.Status = Constants.JobStatus.Error.ToString();
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        var logMsg = BuildLogMessageWhenStagingData(queueItem, exc);

        logger.Log(Msg.Create(LogLevel.Error, logger.Name, base.PrefixJobGuid(logMsg), exc));
    }

    private static string BuildLogMessageWhenStagingData<TException>(Queue queueItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} " +
                    $"FileDate: {queueItem.FileDate} -> Exception details : {httpEx}",
            _ =>
                $"Error staging data in S3 -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.GetType().FullName} - Message: {exc.Message} - STACK {exc.StackTrace}"
        };
    }
    private FileCollectionItem StageReport(Queue queueItem, FileCollectionItem report, Func<JArray, string, DateTime, string, FileCollectionItem> writeToFileSignature, APIEntity entity, string fileName)
    {
        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid(
            $"Staging Metrics Report for raw file: {report.FilePath}; report type {report.SourceFileName};" +
            $" account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID}")));

        FileCollectionItem stagedFile = null;

        switch (report.SourceFileName)
        {
            case "Delivery":
                var reportDataDelivery = GetDeliveryData<DeliveryMetricReport, DeliveryMetrics>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageDelivery(reportDataDelivery, queueItem, writeToFileSignature, fileName, entity);
                break;
            case "DeliveryAppType":
            case "DeliveryLocationRegion":
                var reportDataDeliveryWithTarget = GetDeliveryData<DeliveryMetricReport, DeliveryMetrics>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageDeliveryWithTarget(reportDataDeliveryWithTarget, queueItem, writeToFileSignature, fileName, entity);
                break;
            case "Conversion":
                var reportDataConversion = GetDeliveryData<DeliveryMetricReport, DeliveryMetrics>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageConversion(reportDataConversion, queueItem, writeToFileSignature, fileName);
                break;
            case "DeliveryCatalogSales":
                var reportDataCatalog = GetDeliveryData<DeliveryCatalogSalesMetricReport, DeliveryCatalogSalesMetrics>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageDeliveryCatalogSales(reportDataCatalog, queueItem, writeToFileSignature, fileName, entity);
                break;
            case "AdGroupDims":
                var reportDataAdGroupDim = GetDimData<AdGroupDimReport>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageAdGroupDim(reportDataAdGroupDim.Items, queueItem, writeToFileSignature, fileName);
                break;
            case "AdsDims":
                var reportDataAdsDim = GetDimData<AdsDimReport>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageAdsDim(reportDataAdsDim.Items, queueItem, writeToFileSignature, fileName);
                break;
            case "CampaignDims":
                var reportDataCampaignDim = GetDimData<CampaignDimReport>(report.FilePath, queueItem);
                stagedFile = PinterestService.StageCampaignDim(reportDataCampaignDim.Items, queueItem, writeToFileSignature, fileName);
                break;
        }

        stagedFile.SourceFileName = report.SourceFileName;

        return stagedFile;
    }

    private List<R> GetDeliveryData<R, M>(string rawFile, Queue queueItem) where R : IMetricReport<M>, new()
    {
        var deliveryData = new List<R>();

        string[] paths = new string[]
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), rawFile
        };

        var s3ReportFilePath = RemoteUri.CombineUri(this.baseRawDestUri, paths);

        var s3ReportFileStream = remoteAccessClient.WithFile(s3ReportFilePath).Get();

        using (var txtReader = new StreamReader(s3ReportFileStream))
        {
            var text = txtReader.ReadToEnd();
            if (!string.IsNullOrEmpty(text))
            {
                var jobj = JObject.Parse(text);
                var deliveryReports = jobj.Properties().Select(x => new R { EntityID = x.Name, DeliveryMetrics = JsonConvert.DeserializeObject<List<M>>(x.Value.ToString()) });
                deliveryData.AddRange(deliveryReports);
            }
        }

        return deliveryData;
    }

    private T GetDimData<T>(string rawFile, Queue queueItem) where T : new()
    {
        T deliveryData = new T();

        string[] paths = new string[]
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), rawFile.Substring(rawFile.LastIndexOf(Constants.FORWARD_SLASH) + 1)
        };

        var s3ReportFilePath = RemoteUri.CombineUri(this.baseRawDestUri, paths);

        var s3ReportFileStream = remoteAccessClient.WithFile(s3ReportFilePath).Get();

        using (var txtReader = new StreamReader(s3ReportFileStream))
        {
            var text = txtReader.ReadToEnd();
            if (!string.IsNullOrEmpty(text))
            {
                var report = JsonConvert.DeserializeObject<T>(text);
                deliveryData = report;
            }
        }

        return deliveryData;
    }

    private FileCollectionItem WriteObjectToFile(JArray entity, string entityID, DateTime fileDate, string filename)
    {
        string[] paths = new string[]
        {
            entityID.ToLower(), GetDatedPartition(fileDate), filename
        };

        var filePathUri = RemoteUri.CombineUri(baseStageDestUri, paths);
        var transformedFile = remoteAccessClient.WithFile(filePathUri);
        ETLProvider.SerializeRedshiftJson(entity, transformedFile, new UTF8Encoding(false));

        return new FileCollectionItem
        {
            FilePath = filePathUri.AbsoluteUri,
            FileSize = transformedFile.Length
        };
    }

    private void QueueMetricReports(IBackOffStrategy apiCallsBackOffStrategy, List<ApiReportItem> reportList)
    {
        //get the latest filedate for each entity in queue 
        var maxQueueEntityFileDate = queueItems.Where(x => !x.IsBackfill)
                             .GroupBy(x => x.EntityID)
                             .Select(x => x.OrderByDescending(q => q.FileDate).FirstOrDefault())
                             .ToList();

        var metricsReports = _reports.Where(r => r.ReportSettings.ReportType == "metrics");

        foreach (Queue queueItem in queueItems)
        {
            var hasLocationRegionOptIns = _locationRegionOptIns.Contains(queueItem.EntityID);
            var hasAppTypeOptIns = _appTypeOptIns.Contains(queueItem.EntityID);

            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            //Do not queue unfinished reports
            if (reportList.Any(r => r.QueueID == queueItem.ID))
                continue;

            foreach (var report in metricsReports)
            {
                //If the Entity ID did not opt into either of these reports, skip it
                if (report.ReportSettings.IsDeliveryLocationRegion && !hasLocationRegionOptIns)
                {
                    continue;
                }

                if (report.ReportSettings.IsDeliveryAppType && !hasAppTypeOptIns)
                {
                    continue;
                }

                //initialize dates here, instead of, above because it'll be overwritten by Conversion report. 
                DateTime startDate = queueItem.FileDate;
                DateTime endDate = queueItem.FileDate;

                try
                {
                    if (report.APIReportName.Equals("Conversion"))
                    {
                        //30 day Conversion reports for non-backfill jobs  only if today's Saturday
                        if (!queueItem.IsBackfill)
                        {
                            /* Note about the IF condition below:
                             * 
                             * condition A: <<!DateTime.Today.DayOfWeek.Equals(DayOfWeek.Saturday)>>
                             * condition B: <<!maxQueueEntityFileDate.Contains(queueItem)>>
                             * 
                             * If either condition is TRUE, then create an empty report
                             * When both are FALSE, then create a conversion report
                             * 
                             * Condition A will always be TRUE except on Saturday
                             * 
                             * On Saturday, condition A and B will be FALSE for the latest queue record (eg -1 offset)
                             * 
                             */
                            if (!maxQueueEntityFileDate.Contains(queueItem) || !DateTime.Today.DayOfWeek.Equals(DayOfWeek.Saturday))
                            {
                                //Now that we are resuming unfinished reports. All reports must be in reportList
                                reportList.Add(new ApiReportItem()
                                {
                                    QueueID = queueItem.ID,
                                    FileGuid = queueItem.FileGUID,
                                    ReportToken = "0",
                                    ProfileID = queueItem.EntityID,
                                    FileExtension = report.ReportSettings.FileExtension,
                                    IsReady = false,
                                    IsDownloaded = true,
                                    Status = ReportStatus.FINISHED.ToString(),
                                    Report = report
                                });
                                continue;
                            }
                            else
                            {
                                startDate = queueItem.FileDate.AddDays(-30);
                                endDate = queueItem.FileDate;
                            }
                        }
                    }

                    var apiReport = SendReportRequest(queueItem, report, startDate, endDate);

                    CheckRateLimit(apiReport);

                    var reportItem = new ApiReportItem()
                    {
                        QueueID = queueItem.ID,
                        FileGuid = queueItem.FileGUID,
                        ReportToken = apiReport.Token,
                        ProfileID = queueItem.EntityID,
                        FileExtension = report.ReportSettings.FileExtension,
                        TaskRunDate = DateTime.UtcNow,
                        Report = report,
                        ReportURL = apiReport.URL
                    };
                    reportList.Add(reportItem);

                    LogInfo($"Queue Report: FileGUID: {queueItem.FileGUID}->API Response: {JsonConvert.SerializeObject(apiReport)}");
                }
                catch (HttpRequestException hex)
                {
                    exceptionCount++;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

                    string errorMessage = string.Empty;

                    // Since HttpRequestException does not provide a response directly,
                    // you may need to log additional information based on your specific needs.
                    if (hex.InnerException is WebException webEx)
                    {
                        HttpWebResponse httpWebResponse = webEx.Response as HttpWebResponse;

                        if (httpWebResponse != null)
                        {
                            errorMessage = $"Status Code = {httpWebResponse.StatusCode}";
                            var hasRetryHeader = httpWebResponse.Headers.AllKeys.Contains(resetSecondsHeader);

                            if (httpWebResponse.StatusCode == HttpStatusCode.TooManyRequests && hasRetryHeader)
                            {
                                var resetSecondsString = httpWebResponse.Headers[resetSecondsHeader];
                                ThreadSleep(resetSecondsString);
                            }
                        }
                        else
                        {
                            errorMessage = "httpWebResponse is null";
                        }
                    }
                    else
                    {
                        errorMessage = "HttpRequestException occurred but no inner WebException.";
                    }

                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                            base.PrefixJobGuid($"Http Request Exception Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {startDate}-{endDate}  ->" +
                            $"Error Message: {errorMessage}-> Exception: {hex.Message} -> STACK {hex.StackTrace}")
                        , hex));

                    reportList.RemoveAll(x => x.QueueID == queueItem.ID);
                    break;
                }

                catch (Exception exc)
                {
                    exceptionCount++;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                            base.PrefixJobGuid($"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {startDate}-{endDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}")
                        , exc));

                    reportList.RemoveAll(x => x.QueueID == queueItem.ID);
                    break;
                }
            }
        }
    }

    private MetricReportResponse SendReportRequest(Queue queueItem, APIReport<ReportSettings> report, DateTime? startDate = null, DateTime? endDate = null)
    {
        var settings = new RequestApiReportOptions()
        {
            UrlExtension = report.ReportSettings.Path.TrimEnd('/'),
            MethodType = HttpMethod.Post,
            ProfileID = queueItem.EntityID,
            StartDate = startDate,
            EndDate = endDate,
            DeliveryPath = report.ReportSettings.DeliveryPath ?? "delivery_metrics/"
        };

        if (report.ReportSettings.UseMetrics)
        {
            settings.Metrics = (report.ReportSettings.UseMetrics && report.ReportFields.Any())
                ? report.ReportFields.Where(x => !x.IsDimensionField)
                : null;
        }

        if (report.ReportSettings.UseDimensions)
        {
            settings.Dimensions = (report.ReportSettings.UseDimensions && report.ReportFields.Any())
                ? report.ReportFields.Where(x => x.IsDimensionField)
                : null;
        }

        var endpoint =
            $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{report.ReportSettings.Path.TrimEnd('/')}/{settings.UriPath}";

        settings.SetParameters(report);

        var apiReport = _apiClient.RequestApiReportAsync<MetricReportResponse>(settings).GetAwaiter().GetResult();

        apiReport.URL = endpoint;
        return apiReport;
    }

    private List<ApiReportItem> GetLookupReports(Data.Model.Setup.Lookup lookup)
    {
        List<ApiReportItem> lookupReports = JsonConvert.DeserializeObject<List<ApiReportItem>>(lookup.Value.ToString()).ToList();

        // remove manually deleted Queue records
        var deletedLookups = lookupReports.Where(x => queueIdList.All(queueId => queueId != x.QueueID)).ToList();
        if (deletedLookups.Count != 0)
        {
            LogInfo($"Removing from lookup deleted queues: {string.Join(",", deletedLookups.Select(x => x.QueueID).Distinct())}");
            lookupReports = lookupReports.Except(deletedLookups).ToList();
        }

        // An asynchronous report download URL is good for an hour
        // per doc: "Check the report's status using the token with one of our get async report APIs. If complete, 
        // you get a URL to download the report, good for an hour. Otherwise, you get a "not ready" message."

        // It is unclear how long the token itself is good for, but we should limit anyways because they could eventually be stale
        // and we need a way to remove these from lookup
        var expiryDate = DateTime.UtcNow.AddDays(-totalDaysValid);
        var lookupReportsInvalid = lookupReports.Where(x => x.TaskRunDate != null && x.TaskRunDate < expiryDate).Select(x => x.QueueID).Distinct();
        if (lookupReportsInvalid.Any())
        {
            LogInfo($"Removing from lookup stale reports: {string.Join(",", lookupReportsInvalid)}");
            lookupReports.RemoveAll(x => lookupReportsInvalid.Any(queueId => queueId == x.QueueID));
        }

        // retrieve queue items that were not pulled in as part of the top 100
        var missingQueueIds = lookupReports.Where(x => queueItems.All(q => q.ID != x.QueueID)).Select(x => x.QueueID).Distinct().ToList();
        var missingQueues = JobService.GetQueueItemsByIdList(missingQueueIds, base.SourceId, this.JobLogger.JobLog.JobLogID);
        if (missingQueues.Any())
        {
            LogInfo($"Adding queue IDs from lookup not in top 100: {string.Join(",", missingQueues.Select(x => x.ID))}");
            queueItems.AddRange(missingQueues);
        }

        // remove from lookup any that do not have an "active" queue record
        // ie not pulled in as a result of the previous action - missingQueues
        var inactiveQueueIds = missingQueueIds.Where(queueId => missingQueues.All(queue => queue.ID != queueId)).ToList();
        lookupReports.RemoveAll(x => inactiveQueueIds.Any(queueId => queueId == x.QueueID));

        return lookupReports;
    }

    /// <summary>
    /// When remaining calls reach zero, puts current thread to sleep for specified seconds
    /// </summary>
    /// <param name="apiReport"></param>
    private void CheckRateLimit(MetricReportResponse apiReport)
    {
        //WRITE - 400 calls per minute
        //READ - 500 calls per minute
        //https://developers.pinterest.com/docs/redoc/combined_reporting/#tag/Rate-Limits

        if (!apiReport.Header.TryGetValue(callsRemainingHeader, out var callsRemainingString))
            return;

        if (!apiReport.Header.TryGetValue(resetSecondsHeader, out var resetSecondsString))
            return;

        if (int.TryParse(callsRemainingString, out var callsRemaining))
        {
            if (callsRemaining > 0) return;
        }

        // In v4 - Pinterest now only returns rate limit headers when your request is limited.
        // We log the rate limit information when we have the headers and our going to put the thread to sleep
        // rateLimitHeader (RateLimit-Limit) - provides information about the limit that has been reached including the quota and time window that apply
        // callsRemainingHeader (RateLimit-Remaining) - will be 0, indicating there is no more quota to expend
        // resetSecondsHeader (Retry-After) - minimum time(in seconds) before you should attempt to make another request, but not a guarantee that your request will be successful after this time
        var hasRateLimitHeader = apiReport.Header.TryGetValue(rateLimitHeader, out var rateLimitString);
        if (hasRateLimitHeader)
        {
            logger.Log(Msg.Create(LogLevel.Warn, logger.Name, PrefixJobGuid($"Rate limit reached - RateLimit-Limit:{rateLimitString}; " +
                $"RateLimit-Remaining:{callsRemainingString}; Retry-After:{resetSecondsString}")));
        }
        else
        {
            logger.Log(Msg.Create(LogLevel.Warn, logger.Name, PrefixJobGuid($"Rate limit reached - " +
                $"RateLimit-Remaining:{callsRemainingString}; Retry-After:{resetSecondsString}")));
        }

        ThreadSleep(resetSecondsString);
    }

    private void ThreadSleep(string resetSecondsString)
    {
        if (!int.TryParse(resetSecondsString, out var resetSeconds))
        {
            if (!int.TryParse(SetupService.GetById<Lookup>(Constants.PINTEREST_THREAD_SLEEP)?.Value, out resetSeconds))
                resetSeconds = 60;
        }

        var milliseconds = 1000 * resetSeconds;

        LogInfo($"Putting thread to sleep for {resetSeconds} second(s) before next request.");
        var jobDelay = Task.Run(async () => await Task.Delay(milliseconds));
        jobDelay.Wait();
        return;
    }

    private void SaveState()
    {
        var dbState = SetupService.GetById<Lookup>(dimStateLookupValue);

        if (dbState != null)
        {
            var pinterestStateLookup = new Lookup
            {
                Name = dimStateLookupValue,
                Value = JsonConvert.SerializeObject(dimensionState)
            };
            SetupService.Update(pinterestStateLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(dimStateLookupValue, JsonConvert.SerializeObject(dimensionState));
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

        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}

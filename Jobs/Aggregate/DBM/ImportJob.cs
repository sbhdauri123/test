using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.Data.DataSource.DBM.API;
using Greenhouse.Data.DataSource.DBM.API.Core;
using Greenhouse.Data.DataSource.DBM.API.Resource;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
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
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;
using TimeZoneConverter;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.DBM;

[Export("DV360-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();

    private Uri _baseDestUri;
    private IOrderedEnumerable<OrderedQueue> _queueItems;
    private IEnumerable<Greenhouse.Data.Model.Aggregate.APIReport<ReportSettings>> _apiReports;
    private List<int> _httpStatusNoRetry;
    private List<ApiWebError> _apiWebErrorList;
    private Action<LogLevel, string> _log;
    private Action<LogLevel, string, Exception> _logEx;
    private ScheduledReport _scheduledReports;
    private int _maxPollyRetry;
    private int _s3PauseGetLength;
    private List<ApiReportItem> _unfinishedLookupReports;
    private Lookup _unfinishedReportLookup;
    private IEnumerable<APIEntity> _apiEntities;
    private int _daysThreshold;
    private TimeSpan _maxRuntime;
    private ParallelOptions apiParallelOptions;
    private int _maxAPIRequestPer60s;

    private readonly Stopwatch _runtime = new Stopwatch();
    private Dictionary<string, BackfillDetail> _backfillDetails = new Dictionary<string, BackfillDetail>();
    private List<string> _failedEntities = new List<string>();

    private int _exceptionCounterDaily;
    private int _exceptionCounterBackfill;
    private Auth.OAuthAuthenticator _oAuth;

    private const int _oneMinuteInMilliseconds = 60 * 1000;

    private UnfinishedReportProvider<ApiReportItem> _unfinishedReportProvider;

    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
        this._oAuth = base.OAuthAuthenticator();
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID);
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        //lookup containing an array of http status codes ("303") and if job should retry if error code is matched
        _apiWebErrorList = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.DV360_WEB_ERROR)?.Value) ? new List<ApiWebError>() : ETLProvider.DeserializeType<List<ApiWebError>>(SetupService.GetById<Lookup>(Constants.DV360_WEB_ERROR).Value);
        //Error codes are returned when quotas are exceeded or when an issue prevent the reports from being generated/downloaded 
        //There is no need to retry, mark queue as error
        _httpStatusNoRetry = _apiWebErrorList.Where(x => !x.Retry).Select(x => x.HttpStatusCode).ToList();
        //scheduled reports saved in lookup are managed by Product team
        _scheduledReports = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.DV360_SCHEDULED_REPORTS)?.Value) ? new ScheduledReport() : ETLProvider.DeserializeType<ScheduledReport>(SetupService.GetById<Lookup>(Constants.DV360_SCHEDULED_REPORTS).Value);
        _maxPollyRetry = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.DV360_POLLY_MAX_RETRY)?.Value) ? 10 : int.Parse(SetupService.GetById<Lookup>(Constants.DV360_POLLY_MAX_RETRY)?.Value);
        //pause in ms before getting the size of a file on S3
        //without that pause S3 randomly returns wrong values
        _s3PauseGetLength = int.Parse(SetupService.GetById<Lookup>(Constants.S3_PAUSE_GETLENGTH).Value);
        _daysThreshold = int.Parse(SetupService.GetById<Lookup>(Constants.DV360_NB_DAYS_THRESHOLD).Value);
        _apiEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID, CurrentIntegration.IntegrationID);

        _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
        _logEx = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex));

        _unfinishedReportProvider = new UnfinishedReportProvider<ApiReportItem>(_baseDestUri, _log, _logEx);

        string unfinishedReportsKey = $"{Constants.DV360_UNFINISHED_REPORTS}_{CurrentIntegration.IntegrationID}";

        //DIAT-17921: We are changing the way we are saving unfinished reports.
        //To ensure a smooth transition, load the legacy unfinished report and then save the data the new way
        //We can remove this method once the legacy unfinished lookup are no longer used
        _unfinishedReportLookup = JobService.GetById<Lookup>(unfinishedReportsKey);
        if (!string.IsNullOrEmpty(_unfinishedReportLookup?.Value))
        {
            _unfinishedLookupReports = JsonConvert.DeserializeObject<List<ApiReportItem>>(_unfinishedReportLookup.Value.ToString()).ToList();

            var groupedUnfinishedReports = _unfinishedLookupReports.GroupBy(x => x.FileGuid);
            foreach (var groupedReportList in groupedUnfinishedReports)
            {
                _unfinishedReportProvider.SaveReport(groupedReportList.Key.ToString(), groupedReportList);
            }

            //Delete the Lookup value from the database, as it's no longer needed 
            Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
            repo.Delete(_unfinishedReportLookup);
        }

        _unfinishedLookupReports = _unfinishedReportProvider.LoadUnfinishedReportsFile(_queueItems);
        CleanupReports();

        var backfillDetailsLookup = JobService.GetById<Lookup>(CurrentSource.SourceName + Constants.AGGREGATE_BACKFILL_DETAILS_SUFFIX)?.Value;
        if (backfillDetailsLookup != null)
            _backfillDetails = ETLProvider.DeserializeType<Dictionary<string, BackfillDetail>>(backfillDetailsLookup);

        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.DV360_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _maxAPIRequestPer60s = LookupService.GetLookupValueWithDefault(Constants.DV360_MAX_API_REQUEST_PER_60S, 240);

        int maxParallelAPI = LookupService.GetLookupValueWithDefault(Constants.DV360_MAX_PARALLEL_IMPORT, 3);
        _log(LogLevel.Info, $"ParallelOptions.MaxDegreeOfParallelism API={maxParallelAPI}");
        apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxParallelAPI };
    }

    public void Execute()
    {
        _runtime.Start();
        _log(LogLevel.Info, $"EXECUTE START {base.DefaultJobCacheKey}");

        if (!_queueItems.Any())
        {
            _log(LogLevel.Info, "There are no reports in the Queue");
            return;
        }

        var regularQueues = _queueItems.Where(q => !q.IsBackfill).OrderBy(q => q.RowNumber);
        var backfillQueues = _queueItems.Where(q => q.IsBackfill).OrderBy(q => q.RowNumber);

        // starting regular queues
        _log(LogLevel.Info, "Start Import of Regular queues");
        ImportRegularQueues(regularQueues);

        //Reset the failed entities from Importing Queues.
        //Failures in importing regular queues do not necessarily translate to failures in importing backfill queues
        _failedEntities = new List<string>();

        //starting importing backfill 
        _log(LogLevel.Info, "Start Import of Backfill queues");
        ImportBackfillQueues(backfillQueues);

        // reset any running queues back to pending
        var runningQueues = _queueItems.Where(q => q.Status == Constants.JobStatus.Running.ToString());
        if (runningQueues.Any())
        {
            _log(LogLevel.Info, $"Updating running queue status to 'Pending':{string.Join(',', runningQueues.Select(q => q.ID))}");
            base.UpdateQueueWithDelete(runningQueues, Common.Constants.JobStatus.Pending, false);
        }

        if (_exceptionCounterDaily > 0 || _exceptionCounterBackfill > 0)
        {
            throw new ErrorsFoundException($"Total errors regular: {_exceptionCounterDaily}; Total errors backfill: {_exceptionCounterBackfill} Please check Splunk for more detail.");
        }

        _log(LogLevel.Info, "Import job complete");
    }

    private void ThrottleCalls(List<ApiReportItem> source, int nbItemsPerMinute, Action<ConcurrentBag<ApiReportItem>> action)
    {
        var importStopWatch = Stopwatch.StartNew();

        var subLists = UtilsText.GetSublistFromList(source, nbItemsPerMinute);

        foreach (var list in subLists)
        {
            ConcurrentBag<ApiReportItem> reports = new ConcurrentBag<ApiReportItem>(list);

            action(reports);

            long diff = _oneMinuteInMilliseconds - importStopWatch.ElapsedMilliseconds;

            if (diff > 0)
            {
                _log(LogLevel.Info, $"Queries per minute quota reached - Pausing for {diff} ms");
                Task.Delay((int)diff).Wait();
            }

            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
            {
                //the runtime is greater than the max RunTime
                _log(LogLevel.Info, $"Ellapsed runtime {_runtime.Elapsed} is greater than the max runtime {_maxRuntime}. Stopping additional parallel calls.");
                break;
            }

            importStopWatch.Restart();
        }
    }

    private void ImportBackfillQueues(IEnumerable<IFileItem> backfillQueues)
    {
        if (!backfillQueues.Any())
        {
            _log(LogLevel.Info, "No Backfill Queues to import");
            return;
        }

        List<ApiReportItem> reportList = _unfinishedLookupReports;

        CheckStatusAndDownloadAllReports(reportList, backfillQueues);

        GenerateBackfillReports(backfillQueues, reportList);

        QueueReport(backfillQueues, reportList);
    }

    private void GenerateBackfillReports(IEnumerable<IFileItem> backfillQueues, List<ApiReportItem> reportList)
    {
        var reports = this._apiReports.Where(r => r.ReportSettings.ReportType != null);

        if (!reports.Any())
        {
            _log(LogLevel.Warn, "There are no active reports for BackFill queues");
            return;
        }

        foreach (Queue queueItem in backfillQueues)
        {
            //Do not generate duplicate backfill reports
            if (reportList.Any(x => x.QueueID == queueItem.ID))
            {
                continue;
            }

            DateTime queueStartDate = queueItem.FileDate;
            DateTime queueEndDate = queueItem.FileDate;

            // DIAT-12879 Business Rules: on or after 60 days old: no TYPE_ORDER_ID report
            // meaning that bucket queue (with a date range) needs to be broken up 60 days from now

            // queue.FileDate is based on the apientity's timezone, converting now to the apientity's timezone as well
            var now = ConvertUtcToTimeZone(DateTime.UtcNow, queueItem);

            List<(DateTime startDate, DateTime endDate)> dateList = new List<(DateTime, DateTime)>();

            if (_backfillDetails.ContainsKey(queueItem.FileGUID.ToString()))
            {
                // Bucket details provided
                var details = _backfillDetails[queueItem.FileGUID.ToString()];
                queueStartDate = details.StartDate.Date;
                queueEndDate = details.EndDate.Date;

                var sixtyDaysFromNow = now.AddDays(-_daysThreshold);

                if (queueStartDate <= sixtyDaysFromNow && queueEndDate > sixtyDaysFromNow)
                {
                    // breaking up the bucket into 2 (before and after 60 days)
                    dateList.Add((queueStartDate, sixtyDaysFromNow));
                    dateList.Add((sixtyDaysFromNow.AddDays(1), queueEndDate));
                }
                else
                {
                    // no need to break the bucket
                    dateList.Add((queueStartDate, queueEndDate));
                }
            }
            else
            {
                // no bucket, regular queue for 1 day of data
                dateList.Add((queueStartDate, queueEndDate));
            }

            DateTime maxStartDate = dateList.Max(d => d.startDate);
            bool hasInvalidFloodlightDates = now.Date.Subtract(maxStartDate.Date).TotalDays >= _daysThreshold;

            foreach (var report in reports)
            {
                if (report.ReportSettings.ReportType.Equals(ReportType.FLOODLIGHT.ToString(), StringComparison.InvariantCultureIgnoreCase) && hasInvalidFloodlightDates)
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"Skip Order Report Creation - Max File Date is 60 days or older (Max startdate={maxStartDate.ToLongDateString()}). FileGUID: {queueItem.FileGUID}. DateList: {string.Join(",", dateList.Select(d => $"sd:{d.startDate}|ed:{d.endDate}"))}")));
                    continue;
                }

                foreach (var dates in dateList)
                {
                    var startDate = dates.startDate;
                    var endDate = dates.endDate;

                    if (report.ReportSettings.ReportType.Equals(ReportType.FLOODLIGHT.ToString(), StringComparison.InvariantCultureIgnoreCase) && now.Date.Subtract(startDate.Date).TotalDays >= _daysThreshold)
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                            PrefixJobGuid(
                                $"Skip Order Report File Date is {_daysThreshold} days or older (startdate={startDate.ToLongDateString()}). FileGUID: {queueItem.FileGUID}->Timestamp: {startDate} to {endDate}")));
                        continue;
                    }

                    var reportItem = new ApiReportItem()
                    {
                        QueueID = queueItem.ID,
                        FileGuid = queueItem.FileGUID,
                        ReportName = report.APIReportName,
                        ProfileID = queueItem.EntityID,
                        FileExtension = report.ReportSettings?.FileExtension,
                        StartTime = startDate,
                        EndTime = endDate,
                        Status = Data.DataSource.DCM.ReportStatus.NEW_REPORT
                    };

                    reportList.Add(reportItem);
                }
            }

            SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
        }
    }

    private void SaveUnfinishedReports(List<ApiReportItem> reportList, long queueID, string fileGuid)
    {
        var reportsForQueue = reportList.Where(x => x.QueueID == queueID);

        //Do not save an empty unfinished report file
        if (!reportsForQueue.Any())
        {
            return;
        }

        _unfinishedReportProvider.SaveReport(fileGuid, reportsForQueue);

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Stored unfinished reports for queueID: {queueID} and fileGUID: {fileGuid} in S3")));
    }

    private void ImportRegularQueues(IEnumerable<IFileItem> regularQueues)
    {
        var apiListReport = _apiReports.FirstOrDefault(r => r.ReportSettings.CallType == "getReportResource");

        foreach (Queue queueItem in regularQueues)
        {
            if (_failedEntities.Contains(queueItem.EntityID)) continue;

            ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            var dailyReportSetting = _scheduledReports.DailyReports.Find(r => r.PartnerId.ToString() == queueItem.EntityID);
            if (dailyReportSetting == null)
            {
                LogError($"{queueItem.FileGUID}-Daily Report Schedule missing from lookup - DV360_SCHEDULED_REPORTS for entity {queueItem.EntityID}");
                _exceptionCounterDaily++;
                ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                continue;
            }

            GetDailyReports(apiListReport, queueItem, dailyReportSetting);
        }
    }

    private void CheckStatusAndDownloadAllReports(List<ApiReportItem> reportList, IEnumerable<IFileItem> backfillQueues)
    {
        if (reportList.Count == 0)
        {
            _log(LogLevel.Info, "There are no reports to run");
            return;
        }

        var reportsToCheck = reportList.Where(x => !x.IsReady && x.Status != Data.DataSource.DCM.ReportStatus.FAILED && x.QueryID != 0 && x.ReportID != 0).ToList();
        var reportsOrderedByQueuePriority = from report in reportsToCheck
                                            join queue in _queueItems
                                            on report.QueueID equals queue.ID
                                            orderby queue.RowNumber ascending
                                            select report;

        ThrottleCalls(reportsOrderedByQueuePriority.ToList(), _maxAPIRequestPer60s, (sublist) =>
        {
            Parallel.ForEach(sublist, apiParallelOptions, reportItem =>
            {
                try
                {
                    var reportRequest = new ApiReportRequest()
                    {
                        IsStatusCheck = true,
                        ProfileID = reportItem.ProfileID,
                        QueryID = reportItem.QueryID,
                        ReportID = reportItem.ReportID
                    };

                    RestReport apiReport = _httpClientProvider.SendRequestAndDeserializeAsync<RestReport>(new HttpRequestOptions
                    {
                        Uri = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{reportRequest.UriPath}",
                        Method = HttpMethod.Get,
                        AuthToken = _oAuth.GetAccessToken,
                        ContentType = "application/json",
                    }).GetAwaiter().GetResult();

                    reportItem.Status = UtilsText.ConvertToEnum<Data.DataSource.DCM.ReportStatus>(apiReport.Metadata.Status.State);

                    _log(LogLevel.Info, $"Check Report Status: FileGUID: {reportItem.FileGuid}->API Response: {reportItem.ReportID}->{JsonConvert.SerializeObject(apiReport)}");

                    if (reportItem.Status == Data.DataSource.DCM.ReportStatus.FAILED)
                    {
                        ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                        Interlocked.Increment(ref _exceptionCounterBackfill);
                        reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;
                        LogError($"CheckStatusAndDownloadReport: the service returned status FAILED - failed on queueID: {reportItem.QueueID}->FileGUID: {reportItem.FileGuid} " +
                                                $"for EntityID: {reportItem.ProfileID} Report Id: {reportItem.ReportID} ");
                        return;
                    }

                    if (reportItem.Status != Data.DataSource.DCM.ReportStatus.DONE)
                        return;

                    reportItem.IsReady = true;
                    reportItem.ReportURL = apiReport.Metadata.GoogleCloudStoragePath;
                }
                catch (WebException wex)
                {
                    //get http status code as integer
                    var statusCode = RetrieveHttpStatusCode(wex);
                    //check if status code matches any error codes designated for retry (from lookup)
                    var retryError = _apiWebErrorList.Any(e => e.HttpStatusCode == statusCode && e.Retry);

                    if (retryError)
                    {
                        Interlocked.Increment(ref _exceptionCounterBackfill);
                        LogWebException(reportItem, wex, LogLevel.Warn);
                    }
                    else
                    {
                        reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;
                        ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                        Interlocked.Increment(ref _exceptionCounterBackfill);
                        LogWebException(reportItem, wex);
                    }
                }
                catch (HttpClientProviderRequestException exc)
                {
                    HandleReportItemException(exc, reportItem,
                        $"Error checking report status - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} Report Name ID: {reportItem.ReportID}" +
                        $" - Exception details: {exc}");
                }
                catch (Exception exc)
                {
                    HandleReportItemException(exc, reportItem,
                        $"Error checking report status - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} Report Name ID: {reportItem.ReportID}" +
                        $" - Exception: {exc.Message} - STACK {exc.StackTrace}");
                }//end try catch
            }); //end for  
        });

        SaveUnfinishedReports(reportList);

        var reportsToDownload = reportList.Where(x => x.IsReady && !x.IsDownloaded && x.Status != Data.DataSource.DCM.ReportStatus.FAILED).ToList();

        var reportsToDownloadOrderedByPriority = from report in reportsToDownload
                                                 join queue in _queueItems
                                                 on report.QueueID equals queue.ID
                                                 orderby queue.RowNumber ascending
                                                 select report;

        ThrottleCalls(reportsToDownloadOrderedByPriority.ToList(), _maxAPIRequestPer60s, (sublist) =>
        {
            Parallel.ForEach(sublist, apiParallelOptions, reportItem =>
            {
                reportItem.IsDownloaded = DownloadReportAsync(reportItem, backfillQueues).GetAwaiter().GetResult();

                if (!reportItem.IsDownloaded)
                {
                    reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;
                    reportItem.IsReady = false;
                    ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                }
            });
        });

        SaveUnfinishedReports(reportList);

        foreach (var backfillQueue in backfillQueues)
        {
            var queueReportList = reportList.Where(x => x.QueueID == backfillQueue.ID);
            if (!queueReportList.Any())
            {
                continue;
            }

            bool done = queueReportList.All(x => x.IsDownloaded == true && x.IsReady == true);
            if (done)
            {
                var files = queueReportList.Where(x => x.FileCollection != null).Select(x => x.FileCollection).ToList();
                backfillQueue.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
                backfillQueue.FileSize = files.Sum(x => x.FileSize);
                backfillQueue.DeliveryFileDate = queueReportList.Max(x => x.DeliveryFileDate);
                backfillQueue.Status = Constants.JobStatus.Complete.ToString();
                backfillQueue.StatusId = (int)Constants.JobStatus.Complete;
                JobService.Update((Queue)backfillQueue);
                _unfinishedReportProvider.DeleteReport(backfillQueue.FileGUID.ToString());
            }
        }
    }
    // Helper Method for Handling Exceptions
    private void HandleReportItemException(
        Exception exc,
        ApiReportItem reportItem,
        string message)
    {
        // Update the report item status
        reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;

        // Increment the exception counter
        Interlocked.Increment(ref _exceptionCounterBackfill);

        // Change the queue status to Error
        ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);

        // Log the error message with exception details
        LogError(message, exc);
    }
    private void SaveUnfinishedReports(List<ApiReportItem> reportList)
    {
        var groupedLists = reportList.GroupBy(x => x.FileGuid);
        foreach (var groupedList in groupedLists)
        {
            SaveUnfinishedReports(groupedList.ToList(), groupedList.First().QueueID, groupedList.Key.ToString());
        }
    }

    private void LogWebException(ApiReportItem reportItem, WebException wex, LogLevel logLevel = null)
    {
        HttpWebResponse httpWebResponse = wex?.Response as HttpWebResponse;

        if (httpWebResponse != null)
        {
            string errorMessage = string.Empty;

            try
            {
                using (StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream()))
                {
                    errorMessage = streamReader.ReadToEnd();
                }
            }
            catch
            {
                errorMessage = "HttpWebResponse ResponseStream unavailable.";
            }
            finally
            {
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                    base.PrefixJobGuid($"Web Exception Error Running report status- failed on queueID: {reportItem.QueueID}->FileGUID: {reportItem.FileGuid}->" +
                                       $"for EntityID: {reportItem.ProfileID} Report ID: {reportItem.ReportID}; {reportItem.StartTime}->Error Message: {errorMessage} -> Exception: {wex.Message} -> StackTrace: {wex.StackTrace}")
                    , wex));
            }
        }
        else
        {
            _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                base.PrefixJobGuid($"Exception Error Running report status- failed on queueID: {reportItem.QueueID}->FileGUID: {reportItem.FileGuid} " +
                                   $"for EntityID: {reportItem.ProfileID} Report ID: {reportItem.ReportID} -> Exception: {wex.Message} -> StackTrace: {wex.StackTrace}")
                , wex));
        }
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

    private void GetDailyReports(APIReport<ReportSettings> apiListReport, Queue queueItem, Reports dailyReportSetting)
    {
        var callCounter = 0;

        var reportList = new List<ApiReportItem>();

        try
        {
            foreach (var queryID in dailyReportSetting.QueryIdList)
            {
                var reportItem = new ApiReportItem()
                {
                    QueryID = queryID,
                    FileDate = queueItem.FileDate,
                    FileGuid = queueItem.FileGUID,
                    ProfileID = queueItem.EntityID,
                    QueueID = queueItem.ID
                };
                reportList.Add(reportItem);

                var scheduledReport = GetScheduledReport(queueItem, apiListReport, queryID, callCounter);
                if (string.IsNullOrEmpty(scheduledReport?.Metadata?.Status?.State))
                {
                    _failedEntities.Add(queueItem.EntityID);
                    ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                    _exceptionCounterDaily++;
                    LogError($"FileGUID: {queueItem.FileGUID}; QueueID: {queueItem.ID} FileDate: {queueItem.FileDate};query:{queryID} - Scheduled Report with status was not found--skip checking remaining scheduled reports");
                    break;
                }

                _log(LogLevel.Info, $"{queueItem.FileGUID}-Scheduled Report found (q:{scheduledReport.Key.QueryId}|r:{scheduledReport.Key.ReportId})|status:{scheduledReport.Metadata.Status.State}");
                reportItem.Status = UtilsText.ConvertToEnum<Data.DataSource.DCM.ReportStatus>(scheduledReport.Metadata.Status.State);

                if (reportItem.Status == Data.DataSource.DCM.ReportStatus.FAILED)
                {
                    _failedEntities.Add(queueItem.EntityID);
                    ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                    _exceptionCounterDaily++;
                    LogError($"{reportItem.FileGuid}-Report status returned as FAILED for Query:{queryID}|Report:{reportItem.ReportID}|EntityID:{reportItem.ProfileID}-Marking queue as Error and skipping rest of reports");
                    break;
                }

                if (reportItem.Status != Data.DataSource.DCM.ReportStatus.DONE)
                {
                    ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Pending);
                    _log(LogLevel.Info, $"FileGUID: {queueItem.FileGUID}; QueueID: {queueItem.ID} - Marking queue as Pending - some reports pending for entity {queueItem.EntityID}");
                    break;
                }

                reportItem.IsReady = true;
                var hasValidReportId = long.TryParse(scheduledReport.Key.ReportId, out long latestReportId);
                if (hasValidReportId)
                    reportItem.ReportID = latestReportId;
                reportItem.ReportURL = scheduledReport.Metadata.GoogleCloudStoragePath;

                // match report type based on source file regex matching filename stored in google cloud
                var gcsPath = new Uri(reportItem.ReportURL);
                var originalFileName = gcsPath.AbsolutePath.Split('/').Last();
                SourceFile currentSourceFile = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(originalFileName));

                if (currentSourceFile == null)
                {
                    _failedEntities.Add(queueItem.EntityID);
                    ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
                    _exceptionCounterDaily++;
                    LogError($"{queueItem.FileGUID}-No matching source file for query ID {reportItem.QueryID}|reportID:{reportItem.ReportID}|url:{reportItem.ReportURL}|filename:{originalFileName}");
                    break;
                }

                reportItem.ReportName = currentSourceFile.SourceFileName;

                // add source file name as prefix because dataload job expects this (formerly api report name)
                reportItem.FilePath = $"{currentSourceFile.SourceFileName}_{originalFileName}";
            }

            var reportsToDownload = reportList.Where(x => !string.IsNullOrEmpty(x.FilePath)).ToList();

            ThrottleCalls(reportsToDownload, _maxAPIRequestPer60s, (sublist) =>
            {
                Parallel.ForEach(sublist, apiParallelOptions, new Action<ApiReportItem, ParallelLoopState>((reportItem, state) =>
                {
                    _log(LogLevel.Info, $"FileGUID: {queueItem.FileGUID}; QueueID: {queueItem.ID} - Downloading latest report {reportItem.ReportName} for QueryID: {reportItem.QueryID} ReportID: {reportItem.ReportID}");
                    reportItem.IsDownloaded = DownloadReportAsync(reportItem, (Queue)queueItem).GetAwaiter().GetResult();

                    if (!reportItem.IsDownloaded)
                    {
                        reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;
                        LogError($"FileGUID: {queueItem.FileGUID}; QueueID: {queueItem.ID} - Report {reportItem.ReportName} download failed for entity {queueItem.EntityID}. Stopping Parallel.ForEach.");
                        state.Stop();
                    }
                }));
            });

            if (reportsToDownload.Any(x => x.Status == Data.DataSource.DCM.ReportStatus.FAILED))
            {
                ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                _exceptionCounterDaily += reportsToDownload.Count(x => x.Status == Data.DataSource.DCM.ReportStatus.FAILED);
            }

            bool done = reportList.All(x => x.IsDownloaded == true && x.IsReady == true);
            if (!done)
                return;

            var files = reportList.Where(x => x.FileCollection != null).Select(x => x.FileCollection).ToList();
            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
            queueItem.FileSize = files.Sum(x => x.FileSize);
            queueItem.DeliveryFileDate = reportList.Max(x => x.DeliveryFileDate);
            queueItem.Status = Constants.JobStatus.Complete.ToString();
            queueItem.StatusId = (int)Constants.JobStatus.Complete;
            JobService.Update((Queue)queueItem);
            _log(LogLevel.Info, $"All reports have downloaded, marking queue {queueItem.ID} - {queueItem.FileGUID} as Complete.");
        }
        catch (Exception exc)
        {
            _failedEntities.Add(queueItem.EntityID);
            ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Error);
            _exceptionCounterDaily++;
            LogError($"{queueItem.FileGUID}-Error importing daily queue ID: {queueItem.ID} for EntityID: {queueItem.EntityID} - Exception: {exc.Message} - STACK {exc.StackTrace}");
        }
    }

    private DateTime ConvertUtcToTimeZone(DateTime date, Queue queueItem)
    {
        var apiTimeZoneInfo =
            APIEntityRepository.GetAPIEntityTimeZone(queueItem.EntityID, _apiEntities, CurrentIntegration);
        var apiTimeZone = TZConvert.GetTimeZoneInfo(apiTimeZoneInfo);
        var convertedDate = TimeZoneInfo.ConvertTimeFromUtc(date, apiTimeZone).Date;
        return convertedDate;
    }

    /// <summary>
    /// Get scheduled report by calling GET /doubleclickbidmanager.googleapis.com/v2/queries/{queryID}/reports?orderBy=key.reportId%20desc and match Metadata.ReportDataEndDate to Queue.FileDate
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="apiListReport"></param>
    /// <param name="queryID"></param>
    /// <param name="callCounter"></param>
    /// <returns></returns>
    private RestReport GetScheduledReport(Queue queueItem, APIReport<ReportSettings> apiListReport, int queryID, int callCounter)
    {
        var scheduledReport = new RestReport();
        var apiCallsBackOffStrategy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxPollyRetry
        };

        bool getNextPageResults = true;
        string nextPageUrl = string.Empty;

        while (getNextPageResults)
        {
            getNextPageResults = false;

            string listReportJson = CancellableWebCall<string>(queueItem, () => GetScheduledReportListAsync(apiListReport, queryID, nextPageUrl).GetAwaiter().GetResult(), apiCallsBackOffStrategy, _httpStatusNoRetry, "GetScheduledReport");
            var reportsList = JsonConvert.DeserializeObject<RestReportList>(listReportJson);

            if (reportsList?.Reports == null)
            {
                _log(LogLevel.Info, $"Report list NOT found: FileGUID: {queueItem.FileGUID}->Query ID:{queryID}->API Response: {JsonConvert.SerializeObject(listReportJson)}");
                break;
            }

            scheduledReport = reportsList.Reports.Find(x => new DateTime(x.Metadata.ReportDataEndDate.Year, x.Metadata.ReportDataEndDate.Month, x.Metadata.ReportDataEndDate.Day).Date == queueItem.FileDate.Date);

            if (scheduledReport == null && !string.IsNullOrEmpty(reportsList.NextPageToken))
            {
                getNextPageResults = true;
                nextPageUrl = reportsList.NextPageToken;
            }

            callCounter++;
            ThreadSleep(callCounter);
        }

        return scheduledReport;
    }

    private async Task<string> GetScheduledReportListAsync(APIReport<ReportSettings> apiReport, int queryID, string nextPageUrl = null)
    {
        ApiReportRequest apiRequest = new ApiReportRequest
        {
            IsStatusCheck = true,
            QueryID = queryID
        };

        apiRequest.SetParameters(apiReport, nextPageUrl);
        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = $"{CurrentIntegration.EndpointURI}/{apiRequest.UriPath}",
            Method = HttpMethod.Get,
            AuthToken = _oAuth.GetAccessToken,
            ContentType = MediaTypeNames.Application.Json
        });
    }

    private void QueueReport(IEnumerable<IFileItem> backfillQueues, List<ApiReportItem> reportList)
    {
        var reports = this._apiReports.Where(r => r.ReportSettings.ReportType != null);

        if (!reports.Any())
        {
            _log(LogLevel.Warn, "There are no active reports for BackFill queues");
            return;
        }

        foreach (Queue queueItem in backfillQueues.Where(x => x.Status != Constants.JobStatus.Complete.ToString()))
        {
            if (_failedEntities.Contains(queueItem.EntityID)) continue;
            ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            var numOfCalls = 1;

            foreach (var report in reports)
            {
                var reportsForQueue = reportList.Where(x => x.QueueID == queueItem.ID && x.ReportName == report.APIReportName);

                //If the report has not been created yet (QueryID == 0) or it failed in a different step, we will create/recreate the report here
                var reportsToGenerate = reportsForQueue.Where(x => x.QueryID == 0 || x.Status == Data.DataSource.DCM.ReportStatus.FAILED);
                foreach (var reportToGenerate in reportsToGenerate)
                {
                    var reportRequest = new ApiReportRequest()
                    {
                        ProfileID = queueItem.EntityID,
                        MethodType = System.Net.Http.HttpMethod.Post,
                        ReportDataRange = DataRange.CURRENT_DAY, // NOTE: this is a placeholder. Actual data-range is assigned when the report is run.
                        AdditionalFilters = report.ReportSettings?.AdditionalFilters
                    };

                    reportRequest.SetParameters(report);

                    var endpoint = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{reportRequest.UriPath}";

                    var httpBody = reportRequest.GetReportRequestBody(report);

                    try
                    {
                        RestQuery apiQuery = _httpClientProvider.SendRequestAndDeserializeAsync<RestQuery>(new HttpRequestOptions
                        {
                            Uri = endpoint,
                            Method = HttpMethod.Post,
                            AuthToken = _oAuth.GetAccessToken,
                            ContentType = MediaTypeNames.Application.Json,
                            Content = new StringContent(httpBody),
                        }).GetAwaiter().GetResult();

                        var hasQueryId = long.TryParse(apiQuery.QueryId, out long apiQueryId);

                        if (!hasQueryId)
                            throw new APIReportException($"{queueItem.FileGUID}|No Query ID available for report {report.APIReportName};value returned:{apiQuery.QueryId};entity:{queueItem.EntityID}");

                        reportToGenerate.QueryID = apiQueryId;

                        //This endpoint does not return a status, but we set the status here to reset failed reports
                        reportToGenerate.Status = Data.DataSource.DCM.ReportStatus.NEW_REPORT;
                    }
                    catch (HttpClientProviderRequestException exc)
                    {
                        HandleQueueItemException(exc, queueItem, reportToGenerate,
                            $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} " +
                            $"-> Exception details: {exc}");

                        break;
                    }
                    catch (Exception exc)
                    {
                        HandleQueueItemException(exc, queueItem, reportToGenerate,
                            $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} " +
                            $"-> Exception: {exc.Message} - STACK {exc.StackTrace}");
                        break;
                    }
                    finally
                    {
                        SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                        ThreadSleep(numOfCalls++);
                    }
                }

                var reportsToRun = reportsForQueue.Where(x => x.QueryID != 0 && x.Status == Data.DataSource.DCM.ReportStatus.NEW_REPORT);

                // v2 requires query to be run in a subsequent call after creation
                // able to pass different data-date-ranges in POST body (parent date parameters are default)
                foreach (var reportToRun in reportsToRun)
                {
                    var apiReportRunRequest = new ApiReportRequest()
                    {
                        QueryID = reportToRun.QueryID
                    };

                    var runQuery = new RestRunQuery()
                    {
                        DataRange = new RunDataRange
                        {
                            CustomStartDate = new ResourceDate { Year = reportToRun.StartTime.Year, Month = reportToRun.StartTime.Month, Day = reportToRun.StartTime.Day },
                            CustomEndDate = new ResourceDate { Year = reportToRun.EndTime.Year, Month = reportToRun.EndTime.Month, Day = reportToRun.EndTime.Day }
                        }
                    };

                    var runQueryRequestBody = Newtonsoft.Json.JsonConvert.SerializeObject(runQuery, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                    var runQueryEndpoint = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{apiReportRunRequest.UriPath}";
                    try
                    {
                        RestReport queryReport = _httpClientProvider.SendRequestAndDeserializeAsync<RestReport>(new HttpRequestOptions
                        {
                            Uri = runQueryEndpoint,
                            Method = HttpMethod.Post,
                            AuthToken = _oAuth.GetAccessToken,
                            ContentType = MediaTypeNames.Application.Json,
                            Content = new StringContent(runQueryRequestBody),
                        }).GetAwaiter().GetResult();

                        var hasReportId = long.TryParse(queryReport.Key.ReportId, out long queryReportId);

                        if (!hasReportId)
                            throw new APIReportException($"{queueItem.FileGUID}|No Query ID available for report {report.APIReportName};value returned:{reportToRun.QueryID};entity:{queueItem.EntityID}");

                        reportToRun.ReportID = queryReportId;
                        reportToRun.Status = UtilsText.ConvertToEnum<Data.DataSource.DCM.ReportStatus>(queryReport.Metadata.Status.State);

                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Queue Report: FileGUID: {queueItem.FileGUID}->QueryID:{reportToRun.QueryID}|ReportID:{reportToRun.ReportID}->API Response:{JsonConvert.SerializeObject(queryReport)}->Date Range: {reportToRun.StartTime} to {reportToRun.EndTime}")));
                    }
                    catch (HttpClientProviderRequestException exc)
                    {
                        HandleQueueItemException(exc,
                            queueItem,
                            reportToRun,
                            $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} " +
                            $"-> Exception details: {exc}");

                        break;
                    }
                    catch (Exception exc)
                    {
                        HandleQueueItemException(exc,
                            queueItem,
                            reportToRun,
                            $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} " +
                            $"-> Exception: {exc.Message} - STACK {exc.StackTrace}");

                        break;
                    }
                    finally
                    {
                        SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                        ThreadSleep(numOfCalls++);
                    }
                }
            }
        }
    }

    private void HandleQueueItemException<TException>(TException exc,
        Queue queueItem,
        ApiReportItem reportItem,
        string message) where TException : Exception
    {
        // Update the queue status to Error
        ChangeQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        // Increment the exception counter
        _exceptionCounterBackfill++;

        // Log the error
        LogError(message, exc);

        // Update the report status
        reportItem.Status = Data.DataSource.DCM.ReportStatus.FAILED;

        // Track the failed entity
        _failedEntities.Add(queueItem.EntityID);
    }

    /// <summary>
    /// returns true if report was downloaded successfully.
    /// </summary>
    /// <param name="exceptionCounter"></param>
    /// <param name="reportItem"></param>
    /// <param name="queueItem"></param>
    /// <returns>Returns true if report was downloaded successfully. False otherwise</returns>
    private async Task<bool> DownloadReportAsync(ApiReportItem reportItem, Queue queueItem)
    {
        try
        {
            await using Stream responseStream =
                await _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
                {
                    Uri = reportItem.ReportURL,
                    Method = HttpMethod.Get,
                    ContentType = null,
                    ForceEmptyContent = true
                });

            string[] paths =
            [
                reportItem.ProfileID.ToLower(),
                GetDatedPartition(reportItem.FileDate),
                reportItem.FilePath
            ];

            S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
            StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
            long length = responseStream.Length;
            UploadToS3(incomingFile, rawFile, paths, length);

            //from time to time S3 will return the wrong file size
            //pausing has proven to reduce the probability of this issue happening
            await Task.Delay(_s3PauseGetLength);
            reportItem.FileSize = rawFile.Length;

            if (reportItem.FileSize != length)
            {
                //When s3 file size does not match expected file size from GCS then retry the download 
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(
                    $"Response length of {length} is not equal to s3 file size of {reportItem.FileSize}" +
                    $": FileGUID: {reportItem.FileGuid}->QueryID:{reportItem.QueryID}->ReportID;{reportItem.ReportID}")));
                return false;
            }

            reportItem.FileCollection = new FileCollectionItem()
            {
                FileSize = reportItem.FileSize,
                SourceFileName = reportItem.ReportName,
                FilePath = reportItem.FilePath
            };
            reportItem.DeliveryFileDate = rawFile.LastWriteTimeUtc;

            _log(LogLevel.Info, $"{CurrentSource.SourceName} end DownloadReport - {reportItem.ReportName}: FileGUID: {reportItem.FileGuid}; QueryID: {reportItem.QueryID}; ReportID: {reportItem.ReportID}; fileSize: {reportItem.FileSize}");
            return true;
        }
        catch (HttpClientProviderRequestException exc)
        {
            HandleException($"Error downloading report - failed on queueID: {queueItem.ID} for EntityID: {queueItem.EntityID} " +
                    $"|Exception details : {exc}");
            return false;
        }
        catch (Exception exc)
        {
            HandleException($"Error downloading report - failed on queueID: {queueItem.ID} for EntityID: {queueItem.EntityID} " +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}");
            return false;
        }
    }

    private void HandleException(string message)
    {
        _exceptionCounterDaily++;
        LogError(message);
    }

    /// <summary>
    /// returns true if report was downloaeded successfully. Updates Queue item to error on exception.
    /// </summary>
    /// <param name="reportItem"></param>
    /// <param name="backfillQueues"></param>
    /// <returns>Returns true if report was downloaded successfully. False otherwise</returns>
    private async Task<bool> DownloadReportAsync(ApiReportItem reportItem, IEnumerable<IFileItem> backfillQueues)
    {
        try
        {
            IFileItem queueItem = backfillQueues.FirstOrDefault(q => q.ID == reportItem.QueueID);
            string fileName =
                $"{reportItem.ReportName}_{queueItem.FileName}_{reportItem.ReportID}.{reportItem.FileExtension}";
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"{CurrentSource.SourceName} start DownloadReport: queueID: {queueItem.ID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));

            await using Stream responseStream = await _httpClientProvider.DownloadFileStreamAsync(
                new HttpRequestOptions
                {
                    Uri = reportItem.ReportURL,
                    Method = HttpMethod.Get,
                    ContentType = null,
                    ForceEmptyContent = true
                });

            string[] paths =
            [
                queueItem.EntityID.ToLower(),
                GetDatedPartition(queueItem.FileDate),
                fileName
            ];

            S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
            StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
            long length = responseStream.Length;
            UploadToS3(incomingFile, rawFile, paths, length);

            //from time to time S3 will return the wrong file size
            //pausing has proven to reduce the probability of this issue happening
            await Task.Delay(_s3PauseGetLength);
            reportItem.FileSize = rawFile.Length;

            if (rawFile.Length != length)
            {
                //When s3 file size does not match expected file size from GCS then retry the download 
                //by marking the report-item as not ready and exit here
                _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(
                    $"Response length of {length} is not equal to s3 file size of {rawFile.Length}" +
                    $": FileGUID: {queueItem.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}")));
                reportItem.IsReady = false;
                return false;
            }

            reportItem.FileCollection = new FileCollectionItem
            {
                FileSize = rawFile.Length,
                SourceFileName = reportItem.ReportName,
                FilePath = fileName
            };

            reportItem.DeliveryFileDate = rawFile.LastWriteTimeUtc;

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));
            return true;
        }
        catch (HttpClientProviderRequestException exc)
        {
            ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
            _exceptionCounterBackfill++;
            LogError(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} " +
                $" ReportID: {reportItem.ReportID} FileID: {reportItem.FileID} Report ID: {reportItem.ReportID}" +
                $"|Exception details : {exc}", exc);
            return false;
        }
        catch (Exception exc)
        {
            ChangeQueueStatus(reportItem.QueueID, Constants.JobStatus.Error);
            _exceptionCounterBackfill++;
            LogError(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} " +
                $" ReportID: {reportItem.ReportID} FileID: {reportItem.FileID} Report ID: {reportItem.ReportID}" +
                $"  - Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
            return false;
        }
    }

    /// <summary>
    /// For every 3 requests, puts current thread to sleep for specified milliseconds
    /// </summary>
    /// <param name="count"></param>
    /// <param name="milliseconds"></param>
    private void ThreadSleep(int count)
    {
        //HACK: DBM has quota of 240 requests per minute, so put thread to sleep after every 239 requests to be on the safe side
        //https://developers.google.com/bid-manager/quotas 
        if (count > 0 && count % (_maxAPIRequestPer60s - 1) == 0)
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Putting thread to sleep for {_oneMinuteInMilliseconds} milliseconds before next request as per API documentation.")));
            var jobDelay = Task.Run(async () => await Task.Delay(_oneMinuteInMilliseconds));
            jobDelay.Wait();
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Thread sleep for {_oneMinuteInMilliseconds} milliseconds is Complete")));
        }
        return;
    }

    private void LogError(string errorMessage, Exception exception = null)
    {
        if (exception == null)
        {
            _log(LogLevel.Error, errorMessage);
            return;
        }

        _logEx(LogLevel.Error, errorMessage, exception);
    }

    private void CleanupReports()
    {
        var activeGuids = JobService.GetQueueGuidBySource(CurrentSource.SourceID);

        //Remove any unfinished report files whose queues were deleted
        _unfinishedReportProvider.CleanupReports(_baseDestUri, activeGuids);
    }

    private void ChangeQueueStatus(long queueID, Constants.JobStatus newStatus)
    {
        var queueItem = _queueItems.First(q => q.ID == queueID);
        queueItem.Status = newStatus.ToString();
        queueItem.StatusId = (int)newStatus;
        JobService.UpdateQueueStatus(queueItem.ID, newStatus);
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

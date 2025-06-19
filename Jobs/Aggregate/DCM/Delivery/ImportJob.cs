using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.DAL.DataSource.DCM;
using Greenhouse.Data.DataSource.DCM;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.DCM;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;
using ApiReportItem = Greenhouse.Data.DataSource.DCM.ApiReportItem;
using ApiReportRequest = Greenhouse.Data.DataSource.DCM.ApiReportRequest;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;
using ReportSettings = Greenhouse.Data.DataSource.DCM.ReportSettings;

namespace Greenhouse.Jobs.Aggregate.DCM.Delivery;

// IMPORTANT: NOTE THAT IF DAILY BF ARE REMOVED FROM Source.AggregateInitializeSettings.backfillAggregateDays
// THE WEEKLY COST REPORT WONT BE REQUESTED ANYMORE

[Export("DCM-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    //HARDCODED DB VALUES
    private const string DB_COST_REPORT_NAME = "Cost";
    private const string DIMENSION_API_REPORT_TYPE = "DimensionAPI";

    private readonly Stopwatch _runtime = new Stopwatch();
    private readonly List<string> _entitiesToSkip = new List<string>();
    private Auth.OAuthAuthenticator _oAuth;
    private int _exceptionCount;

    private Uri _baseDestUri;
    private List<OrderedQueue> _queueItems;
    private IEnumerable<APIReport<ReportSettings>> _APIReports;
    private int _maxIdsInRequest;
    private int _costReportLookback;
    private DayOfWeek _costReportRuntimeDay;
    private TimeSpan _maxRuntime;
    private int _maxRunningReports;
    private int _costReportMaxNumberOfDays;
    private int _maxRunningReportsPerAccount;
    private ParallelOptions _apiParallelOptions;

    private ReportState _costReportState;
    private ReportState _dimensionReportState;
    private readonly List<string> _entityCostCreated = new();

    private Action<LogLevel, string> _log;
    private Action<LogLevel, string, Exception> _logEx;

    private UnfinishedReportProvider<ApiReportItem> _unfinishedReportProvider;
    private List<ApiReportItem> _unfinishedReports;

    private string CostReportStateKey => $"{Constants.DCM_COST_REPORT_STATE}_{CurrentIntegration.IntegrationID}";
    private string DimensionReportStateKey => $"{Constants.DCM_DIMENSION_REPORT_STATE}_{CurrentIntegration.IntegrationID}";
    private IHttpClientProvider _httpClientProvider;
    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();

        Logger logger = NLog.LogManager.GetCurrentClassLogger();
        _log = (logLevel, msg) => logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(msg)));
        _logEx = (logLevel, msg, ex) => logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(msg), ex));

        _log(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");
        this._oAuth = base.OAuthAuthenticator();
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)
            .OrderBy(q => q.RowNumber).ToList();
        _APIReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        //the day in which we run the weekly cost report is configurable; setting default to previously hard coded (Saturday)
        var costReportRuntimeLookup = SetupService.GetById<Lookup>(Constants.DCM_COST_REPORT_RUNTIME_DAY);
        _costReportRuntimeDay = string.IsNullOrEmpty(costReportRuntimeLookup?.Value) ? DayOfWeek.Saturday : UtilsText.ConvertToEnum<DayOfWeek>(costReportRuntimeLookup.Value);

        _costReportLookback = LookupService.GetLookupValueWithDefault(Constants.DCM_COST_REPORT_LOOKBACK, 90);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.DCM_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _maxRunningReports = LookupService.GetLookupValueWithDefault(Constants.DCM_MAX_RUNNING_REPORTS, 1);
        _costReportMaxNumberOfDays = LookupService.GetLookupValueWithDefault(Constants.DCM_COST_REPORT_MAX_NUMBER_OF_DAYS, 30);

        _maxRunningReportsPerAccount = LookupService.GetLookupValueWithDefault(Constants.DCM_MAX_RUNNING_REPORT_PER_ACCOUNT, 30);
        _maxIdsInRequest = LookupService.GetLookupValueWithDefault(Constants.DCM_MAX_IDS_IN_REQUEST_SIZE, 100);

        _log(LogLevel.Info, $"DCM_MAX_RUNNING_REPORTS={_maxRunningReports}");

        _unfinishedReportProvider = new UnfinishedReportProvider<ApiReportItem>(_baseDestUri, _log, _logEx);

        this._unfinishedReports = _unfinishedReportProvider.LoadUnfinishedReportsFile(_queueItems);
        CleanupReports();
        _costReportState = LookupService.GetAndDeserializeLookupValueWithDefault(CostReportStateKey, new ReportState());

        // costReportState keeps track of the entities we requested a cost report for, on the day the job is running
        // to prevent requesting that data more than needed
        // if the state is for a previous day, it is reset
        if (_costReportState.DateReportSubmitted < DateTime.Now.Date)
        {
            _costReportState.DateReportSubmitted = DateTime.Now.Date;
            _costReportState.APIEntitiesSubmitted = new HashSet<string>();
        }

        _dimensionReportState = LookupService.GetAndDeserializeLookupValueWithDefault(DimensionReportStateKey, new ReportState());

        if (_dimensionReportState.DateReportSubmitted < DateTime.Now.Date)
        {
            _dimensionReportState.DateReportSubmitted = DateTime.Now.Date;
            _dimensionReportState.APIEntitiesSubmitted = new HashSet<string>();
        }

        //If someone accidentally set the costReportLookback to be less than the costReportMaxNumberOfDays, we will handle it here
        if (_costReportLookback < _costReportMaxNumberOfDays)
        {
            throw new LookupException($"Conflict in Lookup values -> The costReportLookback ({_costReportLookback}) is smaller than costReportMaxNumberOfDays({_costReportMaxNumberOfDays}).");
        }

        int maxParallelAPI = LookupService.GetLookupValueWithDefault(Constants.DCM_MAX_PARALLEL_IMPORT, 3);
        _apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxParallelAPI };
    }

    private void CleanupReports()
    {
        var activeGuids = JobService.GetQueueGuidBySource(CurrentSource.SourceID);

        //Remove any unfinished report files whose queues were deleted
        _unfinishedReportProvider.CleanupReports(_baseDestUri, activeGuids);
    }

    public void Execute()
    {
        _runtime.Start();

        if (_queueItems.Count != 0)
        {
            // initializing reportList with the unfinished reports for the current queues
            List<ApiReportItem> reportList = this._unfinishedReports.Where(x => _queueItems.Select(q => q.ID).Contains(x.QueueID)).ToList();

            try
            {
                // For queue.IsDimOnly: Download Reports
                // For other queues: check unfinished reports and download if ready
                CheckStatusAndDownloadReport(reportList);

                if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) throw new TimeoutException();

                GenerateAndSubmitReports(reportList);

                _runtime.Stop();
            }
            catch (TimeoutException)
            {
                _log(LogLevel.Warn, $"Runtime exceeded time allotted - {_runtime.ElapsedMilliseconds}ms");
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = "Unfinished reports. Some reports were not ready during this Import and will be picked up by the next one.";
            }
            catch (Exception e)
            {
                _logEx(LogLevel.Error, $"Global Catch", e);
                this._exceptionCount++;
            }
            finally
            {
                UpdateQueuesForPending(reportList);
            }

            if (this._exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {this._exceptionCount}; Please check Splunk for more detail.");
            }
        }//end if queue.Any()
        else
        {
            _log(LogLevel.Info, "There are no reports in the Queue");
        }

        _log(LogLevel.Info, "Import job complete");
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

        _log(LogLevel.Info, $"Stored unfinished reports for queueID: {queueID} and fileGUID: {fileGuid} in S3");
    }

    private static void SaveReportState(string reportStateKey, ReportState reportState)
    {
        var lookup = new Lookup
        {
            Name = reportStateKey,
            Value = JsonConvert.SerializeObject(reportState),

            LastUpdated = DateTime.Now,
            IsEditable = false
        };

        Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
        Data.Repositories.LookupRepository.AddOrUpdateLookup(lookup);
    }

    private void UpdateQueuesForPending(List<ApiReportItem> reportList)
    {
        var failedReports = reportList
            .Where(g => g.Status == ReportStatus.FAILED).Select(x => x.QueueID).Distinct();

        //selecting all reports except the ones where at least one of the report failed
        var runningReports = reportList.Where(r => !failedReports.Contains(r.QueueID));
        base.UpdateQueueWithDelete(runningReports.Select(r => new Queue { ID = r.QueueID }), Common.Constants.JobStatus.Pending, false);
    }

    private int RunReport(List<ApiReportItem> reportList)
    {
        int reportSubmitted = 0;

        if (reportList.Count == 0)
        {
            _log(LogLevel.Info, "There are no reports to run");
            return 0;
        }

        foreach (var queueItem in _queueItems)
        {
            if (_entitiesToSkip.Contains(queueItem.EntityID)) { continue; }

            var reportsToRun = reportList.Where(r => r.QueueID == queueItem.ID && !r.IsSubmitted && r.Status != ReportStatus.FAILED);

            if (reportsToRun.Any())
            {
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
            }

            foreach (var reportItem in reportsToRun)
            {
                // the placeholders for empty reports (they have reportid==0) do not count toward the total of running reports
                var runningReports = reportList.Where(r => r.IsSubmitted && !r.IsReady && !r.IsPlaceholder);

                var numOfRunningReports = runningReports.Count();
                _log(LogLevel.Info, $"Running Reports:{numOfRunningReports} max({_maxRunningReports})");

                //check if we reached the maximum of running reports
                if (numOfRunningReports >= _maxRunningReports)
                {
                    _log(LogLevel.Info, $"Maximum number of reports submitted ({_maxRunningReports}). Returning from method.");
                    return reportSubmitted;
                }

                var runningReportsForEntity = runningReports.Count(x => x.ProfileID == queueItem.EntityID);

                if (runningReportsForEntity >= _maxRunningReportsPerAccount)
                {
                    _log(LogLevel.Info, $"Maximum number of reports submitted for entity ({runningReportsForEntity}). Skipping call to Run endpoint.");
                    break;
                }

                if (TimeSpan.Compare(this._runtime.Elapsed, this._maxRuntime) == 1)
                {
                    //the runtime is greater than the max RunTime
                    return reportSubmitted;
                }

                try
                {
                    CallSubmitReportApiAsync(reportItem).GetAwaiter().GetResult();
                    reportSubmitted++;

                    // delay between requests
                    Task.Delay(300).Wait();
                }
                catch (OperationCanceledException) // runtime >= max runtime
                {
                    throw new TimeoutException();
                }
                catch (HttpClientProviderRequestException exc)
                {
                    HandleException(queueItem.ID, reportItem, exc, failEntity: true);
                    break;
                }
                catch (Exception exc)
                {
                    HandleException(queueItem.ID, reportItem, exc, failEntity: true);
                    break;
                }
                finally
                {
                    SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                }
            }
        }

        return reportSubmitted;
    }
    private void HandleException<TException>(long queueId, ApiReportItem reportItem, TException exc, bool failEntity = false) where TException : Exception
    {
        this._exceptionCount++;
        if (failEntity)
        {
            FailEntity(reportItem.ProfileID);
        }
        FailReport(reportItem, queueId);
        // Build log message
        var logMessage = BuildLogMessage(reportItem, exc);

        _logEx(LogLevel.Error, logMessage, exc);
    }
    private static string BuildLogMessage<TException>(ApiReportItem reportItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
            $"Error Running daily report " +
                        $"- failed on queueID: {reportItem.QueueID} " +
                        $"for EntityID: {reportItem.ProfileID} " +
                        $"FileID: {reportItem.FileID}" +
                        $"Report Name: {reportItem.ReportName}  " +
                        $"|Exception details : {httpEx}",
            _ =>
                        $"Error Running daily report - " +
                        $"failed on queueID: {reportItem.QueueID} for EntityID: {reportItem.ProfileID} " +
                        $"FileID: {reportItem.FileID}" +
                        $"Report Name: {reportItem.ReportName}  " +
                        $"- Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }

    private async Task CallSubmitReportApiAsync(ApiReportItem reportItem)
    {
        ApiReportRequest reportRequest = new()
        {
            IsStatusCheck = false,
            ProfileID = reportItem.ProfileID,
            ReportID = reportItem.ReportID
        };

        ReportRunResponse apiReport = await _httpClientProvider.SendRequestAndDeserializeAsync<ReportRunResponse>(
            new HttpRequestOptions
            {
                Uri = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{reportRequest.UriPath}",
                Method = HttpMethod.Post,
                AuthToken = _oAuth.GetAccessToken,
                ContentType = MediaTypeNames.Application.Json,
            });

        reportItem.FileID = apiReport.fileId;
        reportItem.IsSubmitted = true;
        reportItem.IsReady = false;
        reportItem.Status = UtilsText.ConvertToEnum<ReportStatus>(apiReport.status);
        reportItem.TimeSubmitted = DateTime.UtcNow;

        _log(LogLevel.Info, $"Run API Report: FileGUID: {reportItem.FileGuid}->API Response: {reportItem.ReportID}->{JsonConvert.SerializeObject(apiReport)}");
    }

    //DIAT-17920: Attempted to optimize this method by parallelizing the check status and download reports (for non dim only queues). Even when allowing for 5 threads, no performance improvement occurred. 
    //As such, we abandoned efforts to parallelize this method
    private void CheckStatusAndDownloadReport(List<ApiReportItem> reportList)
    {
        var queuesToDelete = new List<long>();

        //Iterate over each queueItems to ensure that we preserve the RowNumber order
        foreach (var queueItem in _queueItems)
        {
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            if (queueItem.IsDimOnly)
            {
                DownloadDimensionReports(queueItem);
                continue;
            }

            List<ApiReportItem> reports = reportList
                .Where(x =>
                    x.QueueID == queueItem.ID &&
                    x.IsSubmitted &&
                    x.Status != ReportStatus.FAILED &&
                    (!x.IsReady || x.FileItem is null)
                )
                .OrderBy(r => r.TimeSubmitted)
                .ToList();

            foreach (var reportItem in reports)
            {
                //Empty reports
                if (!reportItem.IsDownloaded && reportItem.IsPlaceholder)
                {
                    reportItem.IsReady = true;
                    reportItem.IsDownloaded = CreateEmptyReport((Queue)queueItem, reportItem);
                }
                else
                {
                    ApiReportRequest reportRequest = new ApiReportRequest()
                    {
                        IsStatusCheck = true,
                        ProfileID = reportItem.ProfileID,
                        ReportID = reportItem.ReportID,
                        FileID = reportItem.FileID
                    };

                    ReportStatusResponse apiReport;

                    //Check if Report is ready to be downloaded
                    try
                    {
                        apiReport = _httpClientProvider.SendRequestAndDeserializeAsync<ReportStatusResponse>(new HttpRequestOptions
                        {
                            Uri = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{reportRequest.UriPath}",
                            Method = HttpMethod.Get,
                            AuthToken = _oAuth.GetAccessToken,
                            ContentType = MediaTypeNames.Application.Json
                        }).GetAwaiter().GetResult();

                        reportItem.Status = UtilsText.ConvertToEnum<ReportStatus>(apiReport.status);
                        _log(LogLevel.Info, $"Check Report Status: FileGUID: {reportItem.FileGuid}->API Response: {reportItem.ReportID}->{JsonConvert.SerializeObject(apiReport)}");
                    }
                    catch (HttpClientProviderRequestException exc)
                    {
                        this._exceptionCount++;
                        reportItem.Status = ReportStatus.FAILED;

                        _logEx(LogLevel.Error, $"Error checking report status - failed on queueID: {reportItem.QueueID} " +
                            $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.ReportName} - |Exception details : {exc}"
                            , exc);

                        continue;
                    }
                    catch (Exception exc)
                    {
                        this._exceptionCount++;
                        reportItem.Status = ReportStatus.FAILED;

                        _logEx(LogLevel.Error, $"Error checking report status- failed on queueID: {reportItem.QueueID} " +
                            $"for EntityID: {reportItem.ProfileID} Report Name: {reportItem.ReportName}  - Exception: {exc.Message} - STACK {exc.StackTrace}"
                            , exc);

                        continue;
                    }
                    finally
                    {
                        SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                    }

                    //Download the report if it's ready
                    if (reportItem.Status == ReportStatus.REPORT_AVAILABLE)
                    {
                        reportItem.IsReady = true;
                        reportItem.ReportURL = apiReport.urls.apiUrl;

                        reportItem.IsDownloaded = DownloadReportAsync(reportItem).GetAwaiter().GetResult();
                    }
                }

                //If this report downloaded successfully, check to see if all other reports in the queue were downloaded as well
                //If so, we can mark the queue as complete
                if (reportItem.IsDownloaded)
                {
                    var allReportsForQueue = reportList.Where(x => x.QueueID == reportItem.QueueID);

                    if (allReportsForQueue.All(x => x.IsDownloaded && x.IsReady && x.FileItem is not null))
                    {
                        var allFileItems = allReportsForQueue.Select(r => r.FileItem);
                        queueItem.FileCollectionJSON = JsonConvert.SerializeObject(allFileItems);
                        queueItem.FileSize += allFileItems.Sum(r => r.FileSize);

                        queueItem.Status = Constants.JobStatus.Complete.ToString();
                        queueItem.StatusId = (int)Constants.JobStatus.Complete;
                        JobService.Update((Queue)queueItem);

                        reportList.RemoveAll(r => r.QueueID == queueItem.ID);
                        _unfinishedReports.RemoveAll(r => r.QueueID == queueItem.ID);
                        _unfinishedReportProvider.DeleteReport(reportItem.FileGuid.ToString());
                        queuesToDelete.Add(queueItem.ID);
                    }
                }

                SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
            } //end for
        }

        _queueItems.RemoveAll(q => queuesToDelete.Contains(q.ID));
    }

    /// <summary>
    /// Add queue entity ID to list _entitiesToSkip, so other queues will be skipped with same entity
    /// An error message is logged for product to be aware
    /// </summary>
    /// <param name="queueItem"></param>
    private void FailEntity(string entityID)
    {
        if (!_entitiesToSkip.Contains(entityID))
        {
            _entitiesToSkip.Add(entityID);
            _log(LogLevel.Error, $"Adding Entity ID={entityID} to DCM no-call list - this customer will be removed from further requests for this Import job only.");
        }
    }

    private static void FailReport(ApiReportItem reportItem, long queueID)
    {
        JobService.UpdateQueueStatus(queueID, Constants.JobStatus.Error);
        reportItem.Status = ReportStatus.FAILED;
    }

    private async Task<bool> DownloadReportAsync(ApiReportItem reportItem)
    {
        try
        {
            var queueItem = _queueItems.Find(q => q.ID == reportItem.QueueID);
            var reportName = reportItem.PageNumber != null ? $"{reportItem.ReportName}_{reportItem.PageNumber}" : reportItem.ReportName;
            var fileName = $"{reportItem.FileGuid}_{reportName}_{queueItem.FileName}_{reportItem.ReportID}.{reportItem.FileExtension}";

            _log(LogLevel.Info, $"{CurrentSource.SourceName} start DownloadReport: queueID: {queueItem.ID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}");

            await using Stream responseStream = await _httpClientProvider.DownloadFileStreamAsync(
                new HttpRequestOptions
                {
                    Uri = reportItem.ReportURL,
                    Method = HttpMethod.Get,
                    AuthToken = _oAuth.GetAccessToken,
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
            UploadToS3(incomingFile, rawFile, paths);

            reportItem.FileItem = new FileCollectionItem
            {
                FileSize = rawFile.Length,
                SourceFileName = reportItem.ReportName,
                FilePath = fileName
            };

            _log(LogLevel.Info, $"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}");
            return true;
        }
        catch (HttpClientProviderRequestException exc)
        {
            HandleException(reportItem.QueueID, reportItem, exc);
            return false;
        }
        catch (Exception exc)
        {
            HandleException(reportItem.QueueID, reportItem, exc);
            return false;
        }
    }
    private bool CreateEmptyReport(Queue queueItem, ApiReportItem report)
    {
        try
        {
            var fileName = $"{report.FileGuid}_{report.ReportName}_{queueItem.FileName}_{report.ReportID}.{report.FileExtension}";

            string[] paths = new string[]
            {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName
            };

            S3File rawFile = new S3File(RemoteUri.CombineUri(base.GetDestinationFolder(), paths), GreenhouseS3Creds);
            if (rawFile.Exists)
                rawFile.Delete();

            Stream rawFileStream = rawFile.Create();
            rawFileStream.Close();

            report.FileItem = new FileCollectionItem()
            {
                FileSize = rawFile.Length,
                SourceFileName = report.ReportName,
                FilePath = fileName
            };
        }
        catch (Exception exc)
        {
            this._exceptionCount++;
            report.Status = ReportStatus.FAILED;
            _logEx(LogLevel.Error, $"Error creating empty report -> failed on queue ID: {queueItem.ID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate.ToString("yyyy-MM-dd")} -> Exception: {exc.Message} - STACK {exc.StackTrace}"
                , exc);

            return false;
        }
        return true;
    }

    /// <summary>
    /// Submitting reports consists of three steps:
    /// 1. Generating the APIReportItems and saving them to the unfinished report file
    /// 2. Queueing the reports in the API
    /// 3. Taking reports that were successfully queued and posting an API request to Run them
    /// </summary>
    private void GenerateAndSubmitReports(List<ApiReportItem> reportList)
    {
        //Step 1
        GenerateFactReports(reportList);

        //Step 2
        QueueReport(reportList);

        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) throw new TimeoutException();

        //Step 3
        // set IsSubmitted to true for reports submitted to the API Run endpoint
        int reportSubmitted = RunReport(reportList);

        _log(LogLevel.Info, $"Reports Submitted={reportSubmitted}");
    }

    /// <summary>
    /// We are generating ApiReportItems and immediately saving them to the unfinished reports file
    /// </summary>
    private void GenerateFactReports(List<ApiReportItem> reportList)
    {
        //We do not need to generate reports for dimension api reports, as they are handled separately in the Download phase
        var factAPIReports = _APIReports.Where(x => x.ReportSettings.ReportType != DIMENSION_API_REPORT_TYPE);

        var alreadyRequested = reportList.Select(r => r.QueueID).Distinct().ToList();

        foreach (var queueItem in _queueItems.Where(x =>
                    !alreadyRequested.Contains(x.ID) &&
                    !x.IsDimOnly)
                )
        {
            foreach (var apiReport in factAPIReports)
            {
                if (apiReport.APIReportName.Equals(DB_COST_REPORT_NAME, StringComparison.InvariantCultureIgnoreCase))
                {
                    var costReports = GenerateCostReports(queueItem, apiReport);
                    reportList.AddRange(costReports);
                }
                else
                {
                    var reportItem = new ApiReportItem()
                    {
                        QueueID = queueItem.ID,
                        FileGuid = queueItem.FileGUID,
                        ReportName = apiReport.APIReportName,
                        ProfileID = queueItem.EntityID,
                        FileExtension = apiReport.ReportSettings.FileExtension,
                        StartDate = queueItem.FileDate,
                        EndDate = queueItem.FileDate
                    };

                    reportList.Add(reportItem);
                }
            }

            SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
        }
    }

    /// <summary>
    /// We try to leverage the hierarchy here where possible to cut down runtime: https://developers.google.com/doubleclick-advertisers/guides/trafficking_overview
    /// When we use the hierarchy, it's possible to parallelize calls
    /// If it is not possible to get dimension data using a parent Id, then we will ask for data using the Account ID
    /// </summary>
    /// <param name="queueItem"></param>
    private void DownloadDimensionReports(OrderedQueue queueItem)
    {
        if (!queueItem.IsDimOnly)
            return;

        List<FileCollectionItem> fileCollectionItems = new();

        try
        {
            if (_dimensionReportState.APIEntitiesSubmitted.Contains(queueItem.EntityID))
            {
                _log(LogLevel.Info, $"Dimension data for: {queueItem.EntityID} already downloaded today. Skipping.");
                return;
            }

            var advertiserReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Advertiser");
            var advertiserIds = DownloadDimensionReport(advertiserReport, queueItem, fileCollectionItems);

            var creativeReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Creative");
            DownloadDimensionReportsParallel(advertiserIds, queueItem, creativeReport, fileCollectionItems, "advertiserId");

            var campaignReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Campaign");
            var campaignIds = DownloadDimensionReportsParallelWithBatches(advertiserIds, queueItem, campaignReport, fileCollectionItems, "advertiserIds");

            var adsReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Ad");
            DownloadDimensionReportsParallelWithBatches(campaignIds, queueItem, adsReport, fileCollectionItems, "campaignIds");

            var placementReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Placement");
            DownloadDimensionReportsParallelWithBatches(campaignIds, queueItem, placementReport, fileCollectionItems, "campaignIds");

            var floodlightActivityGroupReport = _APIReports.FirstOrDefault(x => x.APIReportName == "FloodlightActivityGroup");
            var floodlightActivityGroupIds = DownloadDimensionReport(floodlightActivityGroupReport, queueItem, fileCollectionItems);

            var floodlightActivityReport = _APIReports.FirstOrDefault(x => x.APIReportName == "FloodlightActivity");
            DownloadDimensionReportsParallelWithBatches(floodlightActivityGroupIds, queueItem, floodlightActivityReport, fileCollectionItems, "floodlightActivityGroupIds");

            var siteReport = _APIReports.FirstOrDefault(x => x.APIReportName == "Site");
            DownloadDimensionReport(siteReport, queueItem, fileCollectionItems);

            var advertiserGroupReport = _APIReports.FirstOrDefault(x => x.APIReportName == "AdvertiserGroup");
            DownloadDimensionReport(advertiserGroupReport, queueItem, fileCollectionItems);

            var placementStrategyReport = _APIReports.FirstOrDefault(x => x.APIReportName == "PlacementStrategy");
            DownloadDimensionReport(placementStrategyReport, queueItem, fileCollectionItems);

            var contentCategoriesReport = _APIReports.FirstOrDefault(x => x.APIReportName == "ContentCategories");
            DownloadDimensionReport(contentCategoriesReport, queueItem, fileCollectionItems);

            var placementGroupReport = _APIReports.FirstOrDefault(x => x.APIReportName == "PlacementGroup");
            DownloadDimensionReport(placementGroupReport, queueItem, fileCollectionItems);

            _dimensionReportState.APIEntitiesSubmitted.Add(queueItem.EntityID);
            SaveReportState(DimensionReportStateKey, _dimensionReportState);

            var manifestFiles = ETLProvider.CreateManifestFiles(queueItem, fileCollectionItems, _baseDestUri, GetDatedPartition);

            queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(manifestFiles);
            queueItem.FileSize += fileCollectionItems.Sum(r => r.FileSize);

            queueItem.Status = Constants.JobStatus.Complete.ToString();
            queueItem.StatusId = (int)Constants.JobStatus.Complete;
            JobService.Update((Queue)queueItem);
        }
        catch (HttpClientProviderRequestException ex)
        {
            this._exceptionCount++;
            _logEx(LogLevel.Error, $"Exception thrown in DownloadDimensionReport. |Exception details : {ex}", ex);
            base.UpdateQueueWithDelete(new List<OrderedQueue> { queueItem }, Common.Constants.JobStatus.Error, false);
        }
        catch (Exception ex)
        {
            this._exceptionCount++;
            _logEx(LogLevel.Error, $"Exception thrown in DownloadDimensionReport. Exception Message: {ex.Message}", ex);
            base.UpdateQueueWithDelete(new List<OrderedQueue> { queueItem }, Common.Constants.JobStatus.Error, false);
        }
    }

    private List<string> DownloadDimensionReport(APIReport<ReportSettings> apiReport, OrderedQueue queueItem, List<FileCollectionItem> fileCollectionItems)
    {
        if (apiReport == null)
        {
            return new List<string>();
        }

        var guid = Guid.NewGuid();
        var pageToken = "";
        var pageNumber = 0;

        var entityIds = new List<string>();
        ReportSettings settings = ETLProvider.DeserializeType<ReportSettings>(apiReport.ReportSettingsJSON);

        var reportRequest = new ApiDimensionRequest
        {
            ProfileID = queueItem.EntityID,
            MethodType = System.Net.Http.HttpMethod.Get
        };

        var endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, null, Array.Empty<string>(), apiReport.ReportFields);

        do
        {
            var fileCollectionItem = new FileCollectionItem
            {
                SourceFileName = apiReport.APIReportName
            };

            var result = DownloadReportToS3Async(reportRequest, apiReport, guid, pageNumber++, endpoint, queueItem, fileCollectionItem).GetAwaiter().GetResult();

            pageToken = result.NextPageToken;
            endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, null, Array.Empty<string>(), apiReport.ReportFields, pageToken);

            if (result.Data.Count != 0)
            {
                entityIds.AddRange(result.Data);

                fileCollectionItems.Add(fileCollectionItem);
            }
        }
        while (!string.IsNullOrEmpty(pageToken));

        return entityIds;
    }

    private ConcurrentBag<string> DownloadDimensionReportsParallelWithBatches(IEnumerable<string> ids, OrderedQueue queueItem, APIReport<ReportSettings> dimensionReport, List<FileCollectionItem> fileCollectionItems, string endpointModification)
    {
        IEnumerable<string[]> batches = ids.Chunk(_maxIdsInRequest);

        ConcurrentBag<string> retrievedIds = new();
        ConcurrentBag<FileCollectionItem> concurrentFileCollectionItems = new();

        ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();

        Parallel.ForEach(batches, _apiParallelOptions, new Action<string[], ParallelLoopState>((ids, state) =>
        {
            //Each thread will create its own unique guid in order to differentiate one download from another
            var guid = Guid.NewGuid();

            var reportRequest = new ApiDimensionRequest
            {
                ProfileID = queueItem.EntityID,
                MethodType = System.Net.Http.HttpMethod.Get
            };

            ReportSettings settings = ETLProvider.DeserializeType<ReportSettings>(dimensionReport.ReportSettingsJSON);

            var endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, endpointModification, ids, dimensionReport.ReportFields);

            var pageToken = "";
            var pageNumber = 0;

            _log(LogLevel.Info, $"{CurrentSource.SourceName} start Download Dimension Data: queueID: {queueItem.ID}->{dimensionReport.APIReportName}->{endpoint}.");

            do
            {
                try
                {
                    var fileCollectionItem = new FileCollectionItem
                    {
                        SourceFileName = dimensionReport.APIReportName
                    };

                    if (!int.TryParse(SetupService.GetById<Lookup>(Constants.LINKEDIN_POLLY_MAX_RETRY)?.Value, out int maxRetry))
                    {
                        maxRetry = 3;
                    }

                    var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
                    {
                        Counter = 0,
                        MaxRetry = maxRetry
                    };

                    DimensionResponse result = null;

                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), apiCallsBackOffStrategy, _runtime, _maxRuntime);
                    cancellableRetry.Execute(() =>
                    {
                        result = DownloadReportToS3Async(reportRequest, dimensionReport, guid, pageNumber++, endpoint, queueItem, fileCollectionItem).GetAwaiter().GetResult();
                    });

                    if (result.Data.Count != 0)
                    {
                        foreach (var resultData in result.Data)
                        {
                            retrievedIds.Add(resultData);
                        }

                        concurrentFileCollectionItems.Add(fileCollectionItem);
                    }

                    pageToken = result.NextPageToken;
                    endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, endpointModification, ids, dimensionReport.ReportFields, pageToken);
                }
                catch (HttpClientProviderRequestException ex)
                {
                    _logEx(LogLevel.Error, $"|Exception details : {ex}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
                catch (Exception ex)
                {
                    _logEx(LogLevel.Error, $"{ex.Message}", ex);
                    exceptions.Enqueue(ex);
                    state.Stop();
                }
            }
            while (!string.IsNullOrEmpty(pageToken));
        }));

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        fileCollectionItems.AddRange(concurrentFileCollectionItems);

        return retrievedIds;
    }

    private string GenerateDimensionEndpoint(string uriPath, string relativeUri, string endpointModification, string[] ids, IEnumerable<APIReportField> apiReportFields, string pageToken = null)
    {
        var endpoint = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{uriPath}/{relativeUri}?";
        var hasAddedElement = false;
        foreach (var id in ids)
        {
            endpoint += hasAddedElement ? $"&{endpointModification}={id}" : $"{endpointModification}={id}";
            hasAddedElement = true;
        }

        //Specify the dimension report fields
        //The pattern is: fields=<nameOfDimension>(<fieldNames>),nextPageToken. The nameOfDimension will match the relativeUri
        if (hasAddedElement)
            endpoint += "&";

        endpoint += $"fields={relativeUri}(id,name";
        foreach (var apiReportField in apiReportFields)
        {
            endpoint += $",{apiReportField.APIReportFieldName}";
        }
        endpoint += "),nextPageToken";

        if (!string.IsNullOrEmpty(pageToken))
            endpoint += $"&pageToken={pageToken}";

        return endpoint;
    }

    private async Task<DimensionResponse> DownloadReportToS3Async(ApiDimensionRequest reportRequest,
        APIReport<ReportSettings> dimensionReport, Guid guid, int pageNumber, string endpoint, OrderedQueue queueItem,
        FileCollectionItem fileCollectionItem)
    {
        DimensionResponse dimensionResponse = new();

        await using Stream responseStream = await _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
        {
            Uri = endpoint,
            Method = HttpMethod.Get,
            AuthToken = _oAuth.GetAccessToken,
            ContentType = MediaTypeNames.Application.Json,
        });

        //Explicitly using UTF8Encoding(false) in the StreamReader/Writers to ensure that the file is encoded in UTF8 and NOT UTF8 BOM
        //(which is incompatible with processing the files in Redshift).

        using StreamReader streamReader = new(responseStream, new UTF8Encoding(false));
        string data = await streamReader.ReadToEndAsync();

        JObject contentObject = JObject.Parse(data);

        //The Relative Uri endpoint matches the name of the property array returned by the API
        JArray contentArray = (JArray)contentObject[$"{dimensionReport.ReportSettings.RelativeUri}"];
        DimensionData dimensionData = new() { Data = contentArray };

        dimensionResponse.NextPageToken = contentObject.SelectToken("$.nextPageToken")?.Value<string>();

        if (dimensionData.Data != null && !dimensionData.Data.Any())
        {
            return dimensionResponse;
        }

        string fileName =
            $"{dimensionReport.APIReportName}_{queueItem.FileName}_{guid}_{pageNumber}.{dimensionReport.ReportSettings.FileExtension}";

        string[] paths =
        [
            queueItem.EntityID.ToLower(),
            GetDatedPartition(queueItem.FileDate),
            fileName
        ];

        S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);

        responseStream.Seek(0, SeekOrigin.Begin);

        await using (StreamWriter streamWriter = new(responseStream, encoding: new UTF8Encoding(false)))
        await using (JsonTextWriter jsonWriter = new(streamWriter))
        {
            JsonSerializer serializer = new() { NullValueHandling = NullValueHandling.Ignore };
            serializer.Serialize(jsonWriter, dimensionData);
            await streamWriter.FlushAsync();
            responseStream.Seek(0, SeekOrigin.Begin);

            StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
            UploadToS3(incomingFile, rawFile, paths);

            fileCollectionItem.FilePath =
                $"{_baseDestUri.AbsoluteUri.TrimStart('/')}/{queueItem.EntityID.ToLower()}/{GetDatedPartition(queueItem.FileDate)}/" +
                fileName;
            fileCollectionItem.FileSize = rawFile.Length;
        }

        foreach (JToken content in contentArray)
        {
            dimensionResponse.Data.Add(content.SelectToken("$.id").Value<string>());
        }

        return dimensionResponse;
    }

    private ConcurrentBag<string> DownloadDimensionReportsParallel(IEnumerable<string> ids, OrderedQueue queueItem, APIReport<ReportSettings> dimensionReport, List<FileCollectionItem> fileCollectionItems, string endpointModification)
    {
        ConcurrentBag<string> retrievedIds = new();
        ConcurrentBag<FileCollectionItem> concurrentFileCollectionItems = new();

        ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();

        Parallel.ForEach(ids, _apiParallelOptions, new Action<string, ParallelLoopState>((id, state) =>
        {
            //Each thread will create its own unique guid in order to differentiate one download from another
            var guid = Guid.NewGuid();

            var reportRequest = new ApiDimensionRequest
            {
                ProfileID = queueItem.EntityID,
                MethodType = System.Net.Http.HttpMethod.Get
            };

            ReportSettings settings = ETLProvider.DeserializeType<ReportSettings>(dimensionReport.ReportSettingsJSON);

            var idInArray = new string[] { id };
            var endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, endpointModification, idInArray, dimensionReport.ReportFields);

            var pageToken = "";
            var pageNumber = 0;

            _log(LogLevel.Info, $"{CurrentSource.SourceName} start Download Dimension Data: queueID: {queueItem.ID}->{dimensionReport.APIReportName}->{endpoint}.");

            do
            {
                var fileCollectionItem = new FileCollectionItem
                {
                    SourceFileName = dimensionReport.APIReportName
                };

                if (!int.TryParse(SetupService.GetById<Lookup>(Constants.LINKEDIN_POLLY_MAX_RETRY)?.Value, out int maxRetry))
                {
                    maxRetry = 3;
                }

                var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
                {
                    Counter = 0,
                    MaxRetry = maxRetry
                };

                try
                {
                    DimensionResponse result = null;

                    var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), apiCallsBackOffStrategy, _runtime, _maxRuntime);
                    cancellableRetry.Execute(() =>
                    {
                        result = DownloadReportToS3Async(reportRequest, dimensionReport, guid, pageNumber++, endpoint, queueItem, fileCollectionItem).GetAwaiter().GetResult();
                    });

                    if (result.Data.Count != 0)
                    {
                        foreach (var resultData in result.Data)
                        {
                            retrievedIds.Add(resultData);
                        }

                        concurrentFileCollectionItems.Add(fileCollectionItem);
                    }

                    pageToken = result.NextPageToken;
                    endpoint = GenerateDimensionEndpoint(reportRequest.UriPath, settings.RelativeUri, endpointModification, idInArray, dimensionReport.ReportFields, pageToken);
                }
                catch (HttpClientProviderRequestException ex)
                {
                    _logEx(LogLevel.Error, $"|Exception details : {ex}", ex);
                    state.Stop();
                }
                catch (Exception ex)
                {
                    _logEx(LogLevel.Error, $"{ex.Message}", ex);
                    state.Stop();
                }
            }
            while (!string.IsNullOrEmpty(pageToken));
        }));

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        fileCollectionItems.AddRange(concurrentFileCollectionItems);

        return retrievedIds;
    }

    private void QueueReport(List<ApiReportItem> reportList)
    {
        foreach (var queueItem in _queueItems.Where(x => !x.IsDimOnly))
        {
            if (_entitiesToSkip.Contains(queueItem.EntityID)) { continue; }

            var reportsForQueue = reportList.Where(x => x.QueueID == queueItem.ID);

            //Resubmit all failed reports for the queue
            var failedReportsForQueue = reportsForQueue.Where(x => x.Status == ReportStatus.FAILED).ToList();
            foreach (var failedReport in failedReportsForQueue)
            {
                var apiReport = _APIReports.FirstOrDefault(x => x.APIReportName == failedReport.ReportName);

                if (apiReport == null)
                {
                    throw new APIReportException("Failed to find matching API Report name");
                }

                try
                {
                    failedReport.ReportID = SendQueueRequestAsync(apiReport, queueItem, failedReport.StartDate, failedReport.EndDate).GetAwaiter().GetResult();

                    //Reset the flags and status
                    failedReport.IsSubmitted = false;
                    failedReport.IsReady = false;
                    failedReport.Status = ReportStatus.NEW_REPORT;
                }
                catch (Exception)
                {
                    this._exceptionCount++;

                    FailEntity(queueItem.EntityID);
                    FailReport(failedReport, queueItem.ID);
                    break;
                }
                finally
                {
                    SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                }
            }

            //Submit reports that have not been queued
            var reportsToQueue = reportsForQueue.Where(x => x.IsPlaceholder && !x.IsSubmitted);
            foreach (var reportToQueue in reportsToQueue)
            {
                var apiReport = _APIReports.FirstOrDefault(x => x.APIReportName == reportToQueue.ReportName);
                if (apiReport == null)
                {
                    throw new APIReportException("Failed to find matching API Report name");
                }

                try
                {
                    reportToQueue.ReportID = SendQueueRequestAsync(apiReport, queueItem, reportToQueue.StartDate, reportToQueue.EndDate).GetAwaiter().GetResult();
                }
                catch (Exception)
                {
                    this._exceptionCount++;

                    FailEntity(queueItem.EntityID);
                    FailReport(reportToQueue, queueItem.ID);
                    break;
                }
                finally
                {
                    SaveUnfinishedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
                }
            }

            //Check if all Cost True Up Reports were successfully submitted. If so, save the Cost Report state
            var costReports = reportsForQueue.Where(x => x.ReportName.Equals(DB_COST_REPORT_NAME, StringComparison.InvariantCultureIgnoreCase));
            if (costReports.Any() && costReports.All(r => !r.IsPlaceholder && r.Status != ReportStatus.FAILED))
            {
                _costReportState.APIEntitiesSubmitted.Add(queueItem.EntityID);
                SaveReportState(CostReportStateKey, _costReportState);
            }
        }
    }

    private List<ApiReportItem> GenerateCostReports(Queue queueItem, APIReport<ReportSettings> report)
    {
        var reportList = new List<ApiReportItem>();

        if (DateTime.Today.DayOfWeek.Equals(_costReportRuntimeDay) &&                // cost report true up are only on Saturdays
            queueItem.IsBackfill &&                                                  // only requested for BF so it does not slow down import and processing for dailies                        
            !_costReportState.APIEntitiesSubmitted.Contains(queueItem.EntityID) &&   // if they were not already requested today for that entity
            !_entityCostCreated.Contains(queueItem.EntityID))                      // only request the cost report once per entity during a job run
                                                                                   // _costReportState will keep track of that a well once the report is submitted successfully
                                                                                   // and save that state in the DB to keep track accross job runs

        {
            var costReportStartDate = queueItem.FileDate.AddDays(-(_costReportLookback - 1)); //Subtract 1 because the costReportLookback is inclusive of the startDate

            //DIAT-17920: run into issues when running true up reports for 270 days worth of data,
            //breaking this down into several reports
            var dateBuckets = UtilsDate.CreateDateBuckets(costReportStartDate, queueItem.FileDate, _costReportMaxNumberOfDays);

            int counter = 1;
            foreach (var dateBucket in dateBuckets)
            {
                var reportItem = new ApiReportItem()
                {
                    QueueID = queueItem.ID,
                    FileGuid = queueItem.FileGUID,
                    ReportName = report.APIReportName,
                    ProfileID = queueItem.EntityID,
                    FileExtension = report.ReportSettings.FileExtension,
                    StartDate = dateBucket.startDate,
                    EndDate = dateBucket.endDate,
                    PageNumber = counter++
                };

                reportList.Add(reportItem);
            }

            // adding the entity to the list, to make sure the Cost reports are generated for 1 queue of that entity
            _entityCostCreated.Add(queueItem.EntityID);

            _log(LogLevel.Info, $"Cost report: Created - queueID={queueItem.ID} FileGUID={queueItem.FileGUID}");
        }
        else
        {
            _log(LogLevel.Info, $"Cost report: not requested - queueID={queueItem.ID} FileGUID={queueItem.FileGUID}");
            //DIAT-17920: the Cost data is available in Delivery Reports when performing backfills. As such, we will generate an empty report
            reportList.Add(GenerateEmptyReportItem(queueItem, report));
        }

        return reportList;
    }

    /// <summary>
    /// Empty ApiReportItems are used in Processing to communicate to the ETL that there's no data to copy over for Cost Reports. Without them, Processing will fail
    /// </summary>
    /// <returns>An ApiReportItem whose ReportID is 0, indicating that it is an empty report.</returns>
    private static ApiReportItem GenerateEmptyReportItem(Queue queueItem, APIReport<ReportSettings> report)
    {
        return new ApiReportItem()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ReportID = 0,
            ReportName = report.APIReportName,
            ProfileID = queueItem.EntityID,
            FileExtension = report.ReportSettings.FileExtension,
            IsSubmitted = true,
            IsReady = false,
            IsDownloaded = false,
            Status = ReportStatus.REPORT_AVAILABLE,
            TimeSubmitted = DateTime.UtcNow
        };
    }

    private async Task<long> SendQueueRequestAsync(APIReport<ReportSettings> report, Queue queueItem, DateTime startDate, DateTime endDate)
    {
        ReportSettings settings = ETLProvider.DeserializeType<ReportSettings>(report.ReportSettingsJSON);

        var reportRequest = new ApiCreateReportRequest
        {
            ProfileID = queueItem.EntityID,
            StartDate = startDate,
            EndDate = endDate,
            MethodType = HttpMethod.Post
        };
        if (settings.UseMetrics)
        {
            reportRequest.Metrics = (settings.UseMetrics && report.ReportFields.Any()) ? report.ReportFields.Where(x => !x.IsDimensionField) : null;
        }
        if (settings.UseDimensions)
        {
            reportRequest.Dimensions = (settings.UseDimensions && report.ReportFields.Any()) ? report.ReportFields.Where(x => x.IsDimensionField) : null;
        }

        try
        {
            ReportRequestResponse apiReport = await _httpClientProvider.SendRequestAndDeserializeAsync<ReportRequestResponse>(
                new HttpRequestOptions
                {
                    Uri = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{reportRequest.UriPath}",
                    Method = HttpMethod.Post,
                    AuthToken = _oAuth.GetAccessToken,
                    ContentType = MediaTypeNames.Application.Json,
                    Content = new StringContent(reportRequest.GetReportRequestBody(), Encoding.UTF8, MediaTypeNames.Application.Json),
                });

            _log(LogLevel.Info, $"Queue Report: FileGUID: {queueItem.FileGUID} -> ReportName: {report.APIReportName} ->API Response: {apiReport.ReportID}->{JsonConvert.SerializeObject(apiReport)}");
            return apiReport.ReportID;
        }
        catch (HttpClientProviderRequestException exc)
        {
            _logEx(LogLevel.Error, $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} " +
                $"FileDate: {startDate}-{endDate} " +
                $"ReportName: {report.APIReportName} -> |Exception details : {exc}"
                , exc);
            throw;
        }
        catch (Exception exc)
        {
            _logEx(LogLevel.Error, $"Error queueing daily report -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {startDate}-{endDate} ReportName: {report.APIReportName} -> Exception: {exc.Message} - STACK {exc.StackTrace}"
                , exc);
            throw;
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

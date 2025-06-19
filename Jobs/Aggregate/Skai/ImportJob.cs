using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Core;
using Greenhouse.DAL.DataSource.Skai;
using Greenhouse.Data.DataSource.Skai;
using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.Skai;

[Export("Skai-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private readonly Stopwatch _runTime = new();
    private TimeSpan _maxRuntime;
    private Uri _baseDestUri;
    private Uri _baseStageDestUri;
    private SkaiService _skaiService;
    private readonly CancellationTokenSource _cts = new();
    private UnfinishedReportProvider<ApiReportItem> _unfinishedReportProvider;
    private RemoteAccessClient _remoteAccessClient;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private List<OrderedQueue> _queueItems;
    private List<ApiReportItem> _unfinishedReports;
    private List<DimensionState> _dimensionStateList;
    private List<SkaiSavedColumn> _customFields = new();
    private readonly List<string> _invalidCustomerIds = new();
    private int _totalDaysValid;
    private int _exceptionCount;
    private int _warningCount;
    private int _s3PauseGetLength;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));

        LogMessage(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        _remoteAccessClient = base.GetS3RemoteAccessClient();
        //pause in ms before getting the size of a file on S3
        //without that pause S3 randomly returns wrong values
        _s3PauseGetLength = int.Parse(SetupService.GetById<Lookup>(Constants.S3_PAUSE_GETLENGTH).Value);

        #region API Throttle/Retry Pipeline
        int maxRetry = LookupService.GetLookupValueWithDefault(Constants.SKAI_BACKOFF_MAX_RETRY, 2);
        int retryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.SKAI_BACKOFF_DELAY_TIME_IN_SECONDS, 3);
        bool retryUseJitter = LookupService.GetLookupValueWithDefault(Constants.SKAI_BACKOFF_USE_JITTER, true);

        var optionsOnRetry = new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<Exception>(),
            BackoffType = DelayBackoffType.Constant,
            UseJitter = retryUseJitter,
            Delay = TimeSpan.FromSeconds(retryDelayInSeconds),
            MaxRetryAttempts = maxRetry,
            OnRetry = args =>
            {
                LogException(LogLevel.Warn, $"Polly-OnRetry with Exception: {args.Outcome.Exception.Message}. Backoff Policy retry attempt: {args.AttemptNumber}", args.Outcome.Exception);
                _warningCount++;
                return default;
            }
        };

        int callsPerMinute = LookupService.GetLookupValueWithDefault(Constants.SKAI_API_CALLS_PER_MINUTE, 60);
        int callsPerMinuteQueueLimit = LookupService.GetLookupValueWithDefault(Constants.SKAI_API_CALLS_PER_MINUTE_QUEUE_LIMIT, 50);
        int callsPerHour = LookupService.GetLookupValueWithDefault(Constants.SKAI_API_CALLS_PER_HOUR, 2000);
        int callsPerHourQueueLimit = LookupService.GetLookupValueWithDefault(Constants.SKAI_API_CALLS_PER_HOUR_QUEUE_LIMIT, 50);

        // SKAI API limits requests on a fixed window
        // per doc:
        // API calls are limited per user, to the following:
        // 60 requests per minute
        // 2,000 requests per hour
        // When you meet the limit, you receive the following 429 HTTP error: “API rate limit exceeded”.
        // When calling any API endpoint the response headers will show the limits relevant to this user,
        // and the number of remaining calls you can make within the current minute / hour.
        ResiliencePipeline rateLimiter = new ResiliencePipelineBuilder()
            .AddRetry(optionsOnRetry)
            .AddRateLimiter(
            new FixedWindowRateLimiter(
            new FixedWindowRateLimiterOptions
            {
                PermitLimit = callsPerMinute,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = callsPerMinuteQueueLimit,
            })).AddRateLimiter(
            new FixedWindowRateLimiter(
            new FixedWindowRateLimiterOptions
            {
                PermitLimit = callsPerHour,
                Window = TimeSpan.FromHours(1),
                QueueLimit = callsPerHourQueueLimit,
            }))
            .Build();
        #endregion

        _dimensionStateList = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.SKAI_DIM_STATE, new List<DimensionState>());

        SkaiOAuth skaiOAuth = new(CurrentCredential, CurrentIntegration.EndpointURI, HttpClientProvider);
        _customFields = SkaiRepository.GetSkaiColumns().Where(c => c.IsActive).ToList();
        int maxParallelAPI = LookupService.GetLookupValueWithDefault(Constants.SKAI_MAX_PARALLEL_IMPORT, 3);
        var apiParallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxParallelAPI, CancellationToken = _cts.Token };

        // per DIAT-20678, restricting number of custom columns in a report request
        GuardrailConfig defaultGuardail = new()
        {
            MaxTotalColumns = 70,
            ColumnRestrictions = new List<ColumnRestrictionSettings> {
                new (){ MaxColumns = 15, ColumnGroup = "DIMENSIONS"},
                new (){ MaxColumns = 15, ColumnGroup = "CUSTOM_METRICS" }
            }
        };
        GuardrailConfig guardrailConfig = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.SKAI_GUARDRAIL_CONFIG, defaultGuardail);

        SkaiServiceArguments skaiServiceArguments = new()
        {
            LogMessage = this.LogMessage,
            LogException = this.LogException,
            ParallelOptions = apiParallelOptions,
            SkaiOAuth = skaiOAuth,
            CustomColumns = _customFields,
            HttpClientProvider = HttpClientProvider,
            EndpointUri = CurrentIntegration.EndpointURI,
            ResiliencePipeline = rateLimiter,
            GuardrailConfig = guardrailConfig
        };

        _skaiService = new SkaiService(skaiServiceArguments);

        _unfinishedReportProvider = new UnfinishedReportProvider<ApiReportItem>(_baseDestUri, LogMessage, LogException);
        CleanupReports();
        this._unfinishedReports = _unfinishedReportProvider.LoadUnfinishedReportsFile(_queueItems);
        _totalDaysValid = LookupService.GetLookupValueWithDefault(Constants.SKAI_ASYNC_TASK_TOTAL_DAYS_VALID, 30);

        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.SKAI_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
    }

    public void Execute()
    {
        _runTime.Start();
        var dateTimeExecuteStart = DateTime.UtcNow;

        if (_queueItems.Count == 0)
        {
            LogMessage(LogLevel.Info, "There are no items in the Queue.");
            _runTime.Stop();
            return;
        }

        List<ApiReportItem> reportList = _unfinishedReports.Where(unfinishedReport => _queueItems.Select(x => x.ID).Contains(unfinishedReport.QueueID)).ToList();

        foreach (var queueItem in _queueItems)
        {
            if (TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1)
            {
                LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                _warningCount++;
                break;
            }

            if (_invalidCustomerIds.Contains(queueItem.EntityID))
            {
                LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-{queueItem.FileDate}-ServerID {queueItem.EntityID} has been flagged as error earlier in this Import job and will be skipped.");
                continue;
            }

            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            bool hasReports = GenerateReports(queueItem, reportList, _apiReports, dateTimeExecuteStart);

            if (hasReports)
            {
                CheckStatusAndDownloadReport(queueItem, reportList);
            }
        }

        _runTime.Stop();
        LogMessage(LogLevel.Info, $"Import job complete. Run Time: {_runTime.Elapsed}");

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

    /// <summary>
    /// Referencing the unfinished reports, check if current report was previously generated (ie skip request)
    /// and if so, check for any profiles that failed and need to be re-processed (ie avoid re-processing everything).
    /// Returning false => skip new request/ current report was saved in unfinished-reports file
    /// true => need to generate/re-process reports
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="report"></param>
    /// <param name="reportList"></param>
    /// <param name="dateTimeExecuteStart"></param>
    /// <param name="profilesToProcess"></param>
    /// <returns></returns>
    private bool GetReportsToProcess(Queue queueItem, APIReport<ReportSettings> report, List<ApiReportItem> reportList, DateTime dateTimeExecuteStart, out List<SkaiProfile> profilesToProcess)
    {
        bool hasReportsToProcess = false;
        profilesToProcess = new();

        // there is no endpoint to retrieve profiles
        // 1) get profile from dti_skai.profiles table to get all columns
        if (report.ReportSettings.ReportType == ReportType.column)
        {
            var isTrueUpScheduled = base.IsTrueUpScheduled(dateTimeExecuteStart, report.ReportSettings.ScheduleSetting.TrueUpDetails);
            if (!isTrueUpScheduled)
                return false;

            // Get all profiles saved in Redshift and use this list to get all available columns
            // If it is a new entity, then we will be unable to retrieve profiles until at least one queue has been processed
            var allSavedProfiles = SkaiRepository.GetAllSkaiProfiles(queueItem.EntityID);
            if (!allSavedProfiles.Any())
            {
                LogMessage(LogLevel.Info, $"No Profiles found-> details: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}");
                return false;
            }
            profilesToProcess.AddRange(allSavedProfiles);
            return true;
        }

        // we get the active profiles based on which custom fields are enabled
        var profiles = _customFields.Where(x => x.IsActive && UtilsText.ConvertToEnum<ReportEntity>(x.Entity) == report.ReportSettings.Entity && x.ServerID.ToString() == queueItem.EntityID)
            .Select(y => y.ProfileID).Distinct()
            .Select(f => new SkaiProfile { ServerID = queueItem.EntityID, ProfileID = f.ToString() });

        // 2) remove any "stale" fusion report tokens = older than x days
        if (report.ReportSettings.ReportType == ReportType.fusion)
        {
            // no documentation, but we will just say an asynchronous run id is valid for 30 days, after which you cannot query for its status or download its output.
            // we remove these from lookup
            var minus30Date = DateTime.UtcNow.AddDays(-_totalDaysValid);
            var staleReportItemIndex = reportList.FindIndex(x => x.QueueID == queueItem.ID && x.ApiReportType == report.ReportSettings.ReportType
                && x.ApiReportEntity == report.ReportSettings.Entity && x.TimeSubmitted != null && x.TimeSubmitted < minus30Date);

            if (staleReportItemIndex > -1)
            {
                LogMessage(LogLevel.Info, $"Removing from lookup stale report: {reportList[staleReportItemIndex]}");
                reportList.RemoveAt(staleReportItemIndex);
            }
        }

        var unfinishedReports = reportList.Where(x => x.QueueID == queueItem.ID && x.ApiReportType == report.ReportSettings.ReportType && x.ApiReportEntity == report.ReportSettings.Entity);

        // 3) no unfinished reports exist - generate new reports
        if (!unfinishedReports.Any())
        {
            profilesToProcess.AddRange(profiles);
            return true;
        }

        // 4) find any profiles that need to be re-processed
        var failedReports = unfinishedReports.Where(x => x.IsFailed);
        if (failedReports.Any())
        {
            hasReportsToProcess = true;
            var profilesToReprocess = profiles.Where(p => failedReports.Select(x => x.ProfileID).Contains(p.ProfileID));
            profilesToProcess.AddRange(profilesToReprocess);
        }

        // fusion reports are not at profile level, so we Exit method here after checking
        // if saved report failed or not
        if (report.ReportSettings.ReportType == ReportType.fusion)
            return hasReportsToProcess;

        // 5) find any profiles missing from cached reports for any non-fusion report
        var missingProfiles = profiles.Where(p => !unfinishedReports.Select(x => x.ProfileID).Contains(p.ProfileID));
        if (missingProfiles.Any())
        {
            hasReportsToProcess = true;
            profilesToProcess.AddRange(missingProfiles);
        }

        return hasReportsToProcess;
    }

    private bool GenerateReports(Queue queueItem, List<ApiReportItem> reportList, IEnumerable<APIReport<ReportSettings>> reports, DateTime dateTimeExecuteStart)
    {
        foreach (var report in reports.OrderBy(r => r.ReportSettings.ReportType))
        {
            if (TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1)
            {
                LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                _warningCount++;
                break;
            }

            bool hasReportsToProcess = GetReportsToProcess(queueItem, report, reportList, dateTimeExecuteStart, out List<SkaiProfile> profilesToProcess);

            if (!hasReportsToProcess)
            {
                LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-Found no reports to generate for {report.APIReportName}. Run Time: {_runTime.Elapsed}");
                continue;
            }

            LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-Begin queueing reports for {report.APIReportName}-Total Profiles:{profilesToProcess.Count}. Run Time: {_runTime.Elapsed}");

            try
            {
                if (report.ReportSettings.ReportType == ReportType.fusion)
                {
                    // queue fusion reports that are retrieved asynchronously
                    var fusionReportTask = _skaiService.RequestFusionReportAsync(queueItem, report);
                    var fusionReport = fusionReportTask.Result;
                    AddCachedReports(queueItem, report, reportList, new List<ApiReportItem> { fusionReport });
                }
                else if (report.ReportSettings.ReportType == ReportType.custom)
                {
                    if (profilesToProcess.Count == 0)
                        continue;
                    var customReportsTask = _skaiService.DownloadAllProfileMetricsAsync(profilesToProcess, queueItem, report, saveFileAction: (profileID, counter, apiReportItem, stream) => S3UploadStream(queueItem, profileID, counter, apiReportItem, stream, report));
                    var customReports = customReportsTask.Result;

                    CreateRedshiftManifestFile(queueItem, report, customReports);

                    AddCachedReports(queueItem, report, reportList, customReports);
                }
                else if (report.ReportSettings.ReportType == ReportType.column)
                {
                    if (_dimensionStateList.All(s => !s.AccountId.Equals(queueItem.EntityID, StringComparison.InvariantCultureIgnoreCase)))
                    {
                        _dimensionStateList.Add(new DimensionState { AccountId = queueItem.EntityID.ToLower() });
                    }

                    var dimensionState = _dimensionStateList.First(s => s.AccountId.Equals(queueItem.EntityID, StringComparison.InvariantCultureIgnoreCase));

                    // dimensions are downloaded only once a day
                    if (dimensionState.StatusDate?.Date >= dateTimeExecuteStart.Date)
                        continue;

                    LogMessage(LogLevel.Info, $"{CurrentSource.SourceName} start DownloadColumns - queueID: {queueItem.ID}->{report.APIReportName}.");

                    var columnReportsTask = _skaiService.DownloadAllAvailableColumnsAsync(profilesToProcess, queueItem, report,
                        saveFileAction: (profileID, apiReportItem, stream) => S3UploadStream(queueItem, profileID, null, apiReportItem, stream, report),
                        transformDataAction: (profile, responseObject) =>
                        {
                            var fileName = $"{queueItem.FileGUID}_{report.APIReportName}_{queueItem.EntityID}_{profile.ProfileID}";
                            var skaiColumnTypes = GetColumnTypes(responseObject.ColumnsInfo, profile);
                            var objectToSerialize = JArray.FromObject(skaiColumnTypes);
                            string[] stagePaths = new string[]
                            {
                                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), $"{fileName}_staged.json"
                            };

                            IFile transformedFile = _remoteAccessClient.WithFile(RemoteUri.CombineUri(_baseStageDestUri, stagePaths));
                            ETLProvider.SerializeRedshiftJson(objectToSerialize, transformedFile);
                            return new FileCollectionItem()
                            {
                                FileSize = transformedFile.Length,
                                SourceFileName = report.APIReportName.ToLower(),
                                FilePath = $"{fileName}_staged.json"
                            };
                        });
                    var columnReports = columnReportsTask.Result;

                    CreateRedshiftManifestFile(queueItem, report, columnReports, true);

                    AddCachedReports(queueItem, report, reportList, columnReports);

                    var allColumnReportEntities = reportList.Where(x => x.ApiReportType == ReportType.column && x.QueueID == queueItem.ID).Select(y => y.ApiReportEntity).Distinct().ToList();
                    var activeReportEntities = reports.Where(x => x.ReportSettings.ReportType == ReportType.column).Select(y => y.ReportSettings.Entity).Distinct().ToList();

                    if (allColumnReportEntities.Count == activeReportEntities.Count)
                    {
                        // update dim state with the current status date
                        dimensionState.StatusDate = dateTimeExecuteStart.Date;
                        // save the date in order to prevent other queues re-pulling same reports
                        SaveDimensionState();
                    }
                }
            }
            catch (AggregateException ae)
            {
                HandleException(queueItem, report.APIReportName, nameof(GenerateReports), ae);
                return false;
            }
            catch (HttpClientProviderRequestException hex)
            {
                HandleException(queueItem, report.APIReportName, nameof(GenerateReports), hex);
                return false;
            }
            catch (WebException wex)
            {
                HandleException(queueItem, report.APIReportName, nameof(GenerateReports), wex);
                return false;
            }
            catch (Exception exc)
            {
                HandleException(queueItem, report.APIReportName, nameof(GenerateReports), exc);
                return false;
            }
            finally
            {
                SaveCachedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
            }
        }

        LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-End queueing reports. Run Time: {_runTime.Elapsed}");

        return true;
    }
    private void HandleException<TException>(Queue queueItem, string reportName, string methodName, TException exc) where TException : Exception
    {
        _exceptionCount++;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
        LogException(queueItem, reportName, methodName, exc);
        this._invalidCustomerIds.Add(queueItem.EntityID);
    }

    private void LogException<TException>(Queue queueItem, string reportName, string methodName, TException exc) where TException : Exception
    {
        switch (exc)
        {
            case HttpClientProviderRequestException httpEx:
                LogException(LogLevel.Error, $"HttpClientProviderRequestException Error in {methodName} -> failed on: {queueItem.FileGUID} " +
                       $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Report:{reportName} ->" +
                       $"|Exception details : {httpEx}", httpEx);
                break;
            case WebException wex:
                LogException(LogLevel.Error, $"Web Exception Error in {methodName} -> failed on: {queueItem.FileGUID} " +
                       $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Report:{reportName} ->" +
                       $"Error -> Exception: {wex.Message} -> STACK {wex.StackTrace}", wex);
                break;
            case AggregateException ae:
                foreach (var ex in ae.InnerExceptions)
                {
                    LogException(LogLevel.Error,
                        $"Aggregate-InnerException-Error in {methodName} - failed on queueID: {queueItem.ID} " +
                        $"for EntityID: {queueItem.EntityID} Report Name: {reportName} - Exception: {ex.Message} - STACK {ex.StackTrace}", ex);
                }
                break;
            default:
                LogException(LogLevel.Error, $"Error in {methodName} -> failed on: {queueItem.FileGUID} " +
                       $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Report:{reportName} ->" +
                       $" Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                break;
        }
    }
    private void CheckStatusAndDownloadReport(Queue queueItem, List<ApiReportItem> reportList)
    {
        if (reportList.Count == 0)
        {
            LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-There are no fusion reports to download");
            return;
        }

        #region failed/pending status
        FusionReportStatus[] failedStatus = new FusionReportStatus[]{
                    FusionReportStatus.COMPLETED_WITH_ERRORS,
                    FusionReportStatus.PARTIALLY_COMPLETED,
                    FusionReportStatus.FAILED,
                    FusionReportStatus.ABORTED,
                    FusionReportStatus.UNKNOWN,
                    FusionReportStatus.FAILED_DATA_NOT_AVAILABLE,
                    FusionReportStatus.FAILED_DATA_NOT_READY
                    };

        FusionReportStatus[] pendingStatus = new FusionReportStatus[] {
                    FusionReportStatus.PENDING,
                    FusionReportStatus.RUNNING
                    };
        #endregion

        foreach (var reportItem in reportList.Where(x => x.QueueID == queueItem.ID && !x.IsReady && !x.IsFailed).OrderBy(r => r.TimeSubmitted))
        {
            try
            {
                if (TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1)
                {
                    LogMessage(LogLevel.Warn, $"Runtime exceeded time allotted - {_runTime.ElapsedMilliseconds}ms");
                    _warningCount++;
                    break;
                }

                LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-Begin status check for report {reportItem.ReportName}. Run Time: {_runTime.Elapsed}");

                var checkStatusTask = _skaiService.CheckReportStatusAsync(reportItem);
                var statusResponse = checkStatusTask.Result;
                LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-Report status:{statusResponse.Status} for report {reportItem.ReportName}");

                if (reportItem.Status == FusionReportStatus.COMPLETED)
                {
                    reportItem.IsReady = true;

                    using Stream responseStream =
                        _skaiService.DownloadReportAsync(reportItem).GetAwaiter().GetResult();

                    string fileName = $"{reportItem.FileGuid}_{reportItem.ReportName}.{reportItem.FileExtension}";

                    string[] paths =
                    [
                        reportItem.ServerID.ToLower(),
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

                    reportItem.FileDate = rawFile.LastWriteTimeUtc;
                    reportItem.IsDownloaded = true;
                }
                else if (failedStatus.Contains(reportItem.Status))
                {
                    reportItem.IsFailed = true;
                }
                else if (pendingStatus.Contains(reportItem.Status))
                {
                    reportItem.IsReady = false;
                }
            }
            catch (AggregateException ae)
            {
                HandleExceptionAndFailReportItem(queueItem, reportItem, nameof(CheckStatusAndDownloadReport), ae);
                break;
            }
            catch (HttpClientProviderRequestException hex)
            {
                HandleExceptionAndFailReportItem(queueItem, reportItem, nameof(CheckStatusAndDownloadReport), hex);
                break;
            }
            catch (WebException wex)
            {
                HandleExceptionAndFailReportItem(queueItem, reportItem, nameof(CheckStatusAndDownloadReport), wex);
                break;
            }
            catch (Exception exc)
            {
                HandleExceptionAndFailReportItem(queueItem, reportItem, nameof(CheckStatusAndDownloadReport), exc);
                break;
            }//end try catch
            finally
            {
                SaveCachedReports(reportList, queueItem.ID, queueItem.FileGUID.ToString());
            }
        } //end for

        LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-End status check. Run Time: {_runTime.Elapsed}");

        var queueReportList = reportList.Where(x => x.QueueID == queueItem.ID);
        bool done = queueReportList.Any() && queueReportList.All(x => x.IsDownloaded && x.IsReady);
        if (!done)
        {
            LogMessage(LogLevel.Info, $"{queueItem.FileGUID}-All fusion reports are not ready to be downloaded. Change queue back to PENDING status.");
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
            return;
        }

        var extractedFiles = queueReportList.Where(x => x.FileItem != null).Select(x => x.FileItem).ToList();
        var groups = extractedFiles.GroupBy(f => f.SourceFileName).Select(g => new { g.Key, List = g.ToList() });
        var files = groups.Select(x => new FileCollectionItem { SourceFileName = x.Key, FileSize = x.List.Sum(f => f.FileSize), FilePath = x.List[0].FilePath });

        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
        queueItem.FileSize = files.Sum(x => x.FileSize);
        queueItem.DeliveryFileDate = queueReportList.Max(x => x.FileDate);

        LogMessage(LogLevel.Debug,
                $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; " +
                $"file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}");

        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update(queueItem);
    }

    private void HandleExceptionAndFailReportItem<TException>(Queue queueItem, ApiReportItem reportItem, string methodName, TException exception) where TException : Exception
    {
        reportItem.IsFailed = true;
        reportItem.IsReady = true;

        HandleException(queueItem, reportItem.ReportName, methodName, exception);
    }

    private void SaveDimensionState()
    {
        var dbState = SetupService.GetById<Lookup>(Constants.SKAI_DIM_STATE);

        if (dbState != null)
        {
            var skaiStateLookup = new Greenhouse.Data.Model.Setup.Lookup
            {
                Name = Constants.SKAI_DIM_STATE,
                Value = JsonConvert.SerializeObject(_dimensionStateList)
            };
            SetupService.Update(skaiStateLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(Constants.SKAI_DIM_STATE, JsonConvert.SerializeObject(_dimensionStateList));
        }
    }

    private void S3UploadStream(Queue queueItem, string profileID, int? counter, ApiReportItem apiReportItem, System.IO.Stream stream, APIReport<ReportSettings> report)
    {
        var fileNamePrefix = $"{queueItem.FileGUID.ToString().ToLower()}_{report.APIReportName}_{queueItem.EntityID}_{profileID}";

        var fileName = counter == null ? fileNamePrefix : $"{fileNamePrefix}_{counter}";

        string[] paths = { queueItem.EntityID, GetDatedPartition(queueItem.FileDate), $"{fileName}.json" };

        var fullpath = RemoteUri.CombineUri(_baseDestUri, paths);

        S3File rawFile = new(fullpath, GreenhouseS3Creds);
        var incomingFile = new StreamFile(stream, GreenhouseS3Creds);
        base.UploadToS3(incomingFile, rawFile, paths, 0, false);

        var newFile = _remoteAccessClient.WithFile(fullpath);

        //from time to time S3 will return the wrong file size
        //pausing has proven to reduce the probability of this issue happening
        Task.Delay(_s3PauseGetLength).Wait();

        var savedReport = new FileCollectionItem { SourceFileName = report.APIReportName, FilePath = newFile.Name, FileSize = newFile.Length };

        apiReportItem.FileItem = savedReport;
    }

    #region Transform column-report-data for Redshift ingestion
    private void CreateRedshiftManifestFile(Queue queue, APIReport<ReportSettings> report, List<ApiReportItem> customReports, bool useStageDirectory = false)
    {
        // Redshift is processing files in stage folder
        // Manifest file will point to files in raw directory
        var customReportFiles = customReports.ConvertAll(x => x.FileItem);

        var manifestFileItem = new FileCollectionItem();
        var manifest = new Data.Model.Setup.RedshiftManifest();

        if (customReportFiles.Count == 0)
            return;

        // assign the path where source files should be processed from
        // use stage path if there have been any transformations
        // otherwise Redshift will process the raw data
        string s3FilePath = useStageDirectory ? this._baseStageDestUri.OriginalString.TrimStart('/') : this._baseDestUri.OriginalString.TrimStart('/');

        foreach (var file in customReportFiles)
        {
            var s3File = $"{s3FilePath}/{queue.EntityID.ToLower()}/{GetDatedPartition(queue.FileDate)}/{file.FilePath}";
            manifest.AddEntry(s3File, true);
        }

        var fileName = $"{queue.FileGUID}_{report.APIReportName}.manifest";
        var manifestPath = GetManifestFilePath(queue, fileName);
        ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);

        manifestFileItem = new FileCollectionItem()
        {
            FileSize = customReportFiles.Sum(file => file.FileSize),
            SourceFileName = report.APIReportName,
            FilePath = fileName
        };

        // Update all items to use manifest file as the filepath instead of raw file
        // This forces filecollection item to use the manifest
        customReports.ForEach(x => x.FileItem.FilePath = manifestFileItem.FilePath);
    }

    private static List<SkaiColumn> GetColumnTypes(ColumnsInfo columnInfo, SkaiProfile profile)
    {
        var skaiColumns = new List<SkaiColumn>();

        var props = typeof(ColumnsInfo).GetProperties().Where(p => p.PropertyType.ToString().Contains("ColumnAttribute"));

        if (!props.Any()) return skaiColumns;

        foreach (var prop in props)
        {
            var columnAttributes = (List<ColumnAttribute>)columnInfo.GetType().GetProperty(prop.Name).GetValue(columnInfo, null);
            if (columnAttributes == null || columnAttributes.Count == 0) continue;
            var columnList = CreateColumnList<SkaiColumn>(profile, columnAttributes, prop.Name);

            skaiColumns.AddRange(columnList);
        }
        return skaiColumns;
    }

    private static List<SkaiColumn> CreateColumnList<SkaiColumn>(SkaiProfile profile, List<ColumnAttribute> columnAttributes, string columnType)
    {
        var columnList = new List<SkaiColumn>();

        columnList.AddRange(columnAttributes.Where(attribute => attribute != null).Select(attribute =>
            (SkaiColumn)Activator.CreateInstance(typeof(SkaiColumn), attribute, columnType, profile)));

        return columnList;
    }

    private string[] GetManifestFilePath(Queue queueItem, string name)
    {
        string[] manifestPath = new string[]
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate)
        };

        var manifestUri = RemoteUri.CombineUri(_baseDestUri, manifestPath);
        return new string[]
        {
            manifestUri.AbsolutePath, name
        };
    }
    #endregion

    #region Unfinished-Report Helpers
    /// <summary>
    /// Adds new reports to the cached (unfinished) reports file.
    /// If previously failed, then it replaces the previous report item
    /// </summary>
    /// <param name="queueItem"></param>
    /// <param name="report"></param>
    /// <param name="unfinishedReports"></param>
    /// <param name="newReports"></param>
    private static void AddCachedReports(Queue queueItem, APIReport<ReportSettings> report, List<ApiReportItem> unfinishedReports, List<ApiReportItem> newReports)
    {
        var cachedReports = unfinishedReports.Where(x => x.QueueID == queueItem.ID && x.ApiReportType == report.ReportSettings.ReportType && x.ApiReportEntity == report.ReportSettings.Entity);
        if (!cachedReports.Any())
        {
            unfinishedReports.AddRange(newReports);
            return;
        }

        if (report.ReportSettings.ReportType == ReportType.fusion)
        {
            // fusion reports are not at the profile level; only server(entity)
            unfinishedReports.RemoveAll(x =>
                x.QueueID == queueItem.ID
                && x.ApiReportType == report.ReportSettings.ReportType
                && x.ApiReportEntity == report.ReportSettings.Entity);
        }
        else
        {
            unfinishedReports.RemoveAll(x =>
                x.QueueID == queueItem.ID
                && x.ApiReportType == report.ReportSettings.ReportType
                && x.ApiReportEntity == report.ReportSettings.Entity
                && newReports.Select(r => r.ProfileID).Contains(x.ProfileID));
        }

        unfinishedReports.AddRange(newReports);
    }

    private void SaveCachedReports(List<ApiReportItem> reportList, long queueID, string fileGuid)
    {
        var reportsForQueue = reportList.Where(x => x.QueueID == queueID);

        //Do not save an empty unfinished report file
        if (!reportsForQueue.Any())
        {
            return;
        }

        _unfinishedReportProvider.SaveReport(fileGuid, reportsForQueue);

        LogMessage(LogLevel.Info, $"Stored unfinished reports for queueID: {queueID} and fileGUID: {fileGuid} in S3");
    }

    private void CleanupReports()
    {
        var activeGuids = JobService.GetQueueGuidBySource(CurrentSource.SourceID);

        //Remove any unfinished report files whose queues were deleted
        _unfinishedReportProvider.CleanupReports(_baseDestUri, activeGuids);
    }
    #endregion

    private void LogMessage(LogLevel logLevel, string message)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
    }

    private void LogException(LogLevel logLevel, string message, Exception exc = null)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
    }
}

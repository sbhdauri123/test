using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Greenhouse.Jobs.TTD;

[Export("TTD-DeliveryDataLoad", typeof(IDragoJob))]

public class DeliveryProcessing : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private readonly static Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private string JobGUID { get { return this.JED.JobGUID.ToString(); } }
    private List<OrderedQueue> _queueItems;
    private Country _country;
    private DatabricksETLJob _databricksEtlJob;
    private readonly CancellationTokenSource _cts = new();
    private int _exceptionCount;
    private int _warningCount;
    private readonly Stopwatch _runTime = new();
    private TimeSpan _maxRuntime;
    private int _errorThreshold;
    private DatabricksJobProvider _databricksJobProvider;
    private bool _isLastQueueItem;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        CurrentIntegration = SetupService.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));
        _queueItems = JobService.GetOrderedQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).ToList();
        _country = JobService.GetById<Country>(CurrentIntegration.CountryID);
        _errorThreshold = LookupService.GetLookupValueWithDefault(Constants.TTD_DELIVERY_ERROR_THRESHOLD_COUNT, 10);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.TTD_DELIVERY_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _databricksJobProvider = CreateDatabricksJobProvider();
        _cts.CancelAfter(_maxRuntime);
    }

    public void Execute()
    {
        try
        {
            _runTime.Start();

            var queueCounter = 0;
            foreach (var queueItem in _queueItems)
            {
                if (TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1)
                {
                    LogMaxRunTimeWarning();
                    break;
                }

                IsLastQueueItem(_queueItems, ref queueCounter);
                queueItem.Status = nameof(Constants.JobStatus.Running);
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                string fileType = queueItem.SourceFileName;
                var basePath = base.GetDestinationFolder();
                string tableName = string.Format(_databricksEtlJob.DatabricksTableName, queueItem.SourceFileName);

                //job paramaters are order specific: s3Protocol, s3RootBucket, fileType, agency, country, fileGUID, rawFilePath, timeZone, partitionColumn, partitions
                //For video events Hour-1 & Hour+1 folder paths are needed to map Impression Ids. For other file types these parameters are ignored by ETL job.
                //Do not submit spark job if the Hour+1 impression files are not present.
                var currentHourFilePath = RawFilePath(basePath, queueItem.EntityID, queueItem.FileDate, queueItem.FileDateHour.Value);
                string hourMinusOneFilePath = String.Empty, hourPlusOneFilePath = String.Empty;
                GetPlusMinusOneHourFolder(basePath, queueItem, out hourMinusOneFilePath, out hourPlusOneFilePath);

                if (fileType.Equals(Constants.VIDEO_EVENT, StringComparison.InvariantCultureIgnoreCase))
                {
                    if (hourPlusOneFilePath.Equals("null", StringComparison.InvariantCultureIgnoreCase))
                    {
                        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, String.Format("{0} -Skipping video event file: {1}.", JobGUID, queueItem.FileName)));
                        continue;
                    }
                    //"It is expected that impression files will not deliver if there is no spend activity in the partner."
                    else if (!FolderExists(hourPlusOneFilePath + "/impression"))
                    {
                        hourPlusOneFilePath = "null";
                    }
                    else if (!hourMinusOneFilePath.Equals("null", StringComparison.InvariantCultureIgnoreCase)
                                && !FolderExists(hourMinusOneFilePath + "/impression"))
                    {
                        hourMinusOneFilePath = "null";
                    }
                }

                // adding the fileguid as the first parameter to help identify the jobrun
                string[] jobParams = new[] { $"FileGUID={queueItem.FileGUID}" , "s3", this.RootBucket, fileType, _country.CountryName, queueItem.FileGUID.ToString(), tableName, currentHourFilePath,
                                            queueItem.EntityID, hourMinusOneFilePath, hourPlusOneFilePath };

                JobRunRequest request = new()
                {
                    JobID = Convert.ToInt64(_databricksEtlJob.DatabricksJobID),
                    JarParams = jobParams
                };

                    _databricksJobProvider.QueueJobAsync(queueItem.ID, request, (queueID, result) => OnJobException(queueID), _cts.Token).GetAwaiter().GetResult();
                    _databricksJobProvider.WaitForMaxRunJobsToCompleteAsync(OnJobCompletion, (queueID, result) => OnJobException(queueID), _cts.Token, _isLastQueueItem).GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                if (IsMaxRunTimeReachedAndTaskCancelled(ex))
                {
                    LogMaxRunTimeWarning();
                }
                else
                {
                    LogException(LogLevel.Error, $"Error caught in Execute. Message:{ex.Message} - STACK {ex.StackTrace}", ex);
                    _exceptionCount++;
                }
            }
            finally
            {
                // reset RUNNING queues back to PENDING
                var runningQueues = _queueItems.Where(x => x.Status.Equals(nameof(Constants.JobStatus.Running), StringComparison.OrdinalIgnoreCase));
                if (runningQueues.Any())
                    base.UpdateQueueWithDelete(runningQueues, Constants.JobStatus.Pending, false);
            }

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
        else if (_warningCount > 0)
        {
            JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
            JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
        }
    }

    #region execute helpers

    private void CancelJobSubmissions()
    {
        if (_exceptionCount < _errorThreshold)
            return;

        LogMessage(LogLevel.Debug, $"Manually stopping job submissions. Reached error threshold with total errors:{_exceptionCount} (Max:{_errorThreshold})");
        _cts.Cancel();
    }

    private static string RawFilePath(Uri basePath, string entityID, DateTime fileDate, int fileHour)
    {
        string[] paths = new string[] { entityID.ToLower(), GetDatedPartition(fileDate), GetHourPartition(fileHour) };
        var srcFileUri = $"{RemoteUri.CombineUri(basePath, paths).OriginalString.TrimStart('/')}";

        return srcFileUri;
    }

    private void GetPlusMinusOneHourFolder(Uri basePath, OrderedQueue queueItem, out string hourMinusOneFilePath, out string hourPlusOneFilePath)
    {
        if (queueItem.FileDateHour.Value == 0)
        {
            hourMinusOneFilePath = RawFilePath(basePath, queueItem.EntityID, queueItem.FileDate.AddDays(-1), 23);
        }
        else
        {
            hourMinusOneFilePath = RawFilePath(basePath, queueItem.EntityID, queueItem.FileDate, queueItem.FileDateHour.Value - 1);
        }

        if (queueItem.FileDateHour.Value == 23)
        {
            hourPlusOneFilePath = RawFilePath(basePath, queueItem.EntityID, queueItem.FileDate.AddDays(1), 0);
        }
        else
        {
            hourPlusOneFilePath = RawFilePath(basePath, queueItem.EntityID, queueItem.FileDate, queueItem.FileDateHour.Value + 1);
        }
        //Pass values as "null" string if the hour directories do not exist. Spark code drops the arguments that are NULL or empty string.
        if (!FolderExists(hourMinusOneFilePath)) hourMinusOneFilePath = "null";
        if (!FolderExists(hourPlusOneFilePath)) hourPlusOneFilePath = "null";
    }

    private bool FolderExists(string dateToVerify)
    {
        Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);
        var s3FolderToVerify = RemoteUri.CombineUri(baseUri, dateToVerify);
        RemoteAccessClient RAC = new(s3FolderToVerify, GreenhouseS3Creds);
        return RAC.WithDirectory().Exists;
    }

    private void IsLastQueueItem(List<OrderedQueue> queues, ref int queueCounter)
    {
        queueCounter++;

        if (queueCounter == queues.Count)
        {
            _isLastQueueItem = true;
        }
    }

    private void OnJobException(long queueID)
    {
        _exceptionCount++;
        CancelJobSubmissions();
        var currentQueue = _queueItems.Find(x => x.ID == queueID);
        currentQueue.Status = Constants.JobStatus.Error.ToString();
        JobService.UpdateQueueStatus(queueID, Constants.JobStatus.Error, false);
        LogMessage(NLog.LogLevel.Error, $"ERROR->FileGUID: {currentQueue.FileGUID};deleting from Partition table fileguid entries.");
        JobService.DeletePartitionByFileGUID(currentQueue.FileGUID.ToString());
    }

    private void OnJobCompletion(DatabricksJobResult jobResult)
    {
        var currentQueue = _queueItems.Find(x => x.ID == jobResult.QueueID);

        if (jobResult.JobStatus == ResultState.SUCCESS)
        {
            LogMessage(NLog.LogLevel.Info, $"SUCCESS->FileGUID: {currentQueue.FileGUID};QueueID: {jobResult.QueueID}; Spark job completed - JobRunID={jobResult.JobRunID} - JobID={_databricksEtlJob.DatabricksJobID}. Job status: {jobResult.JobStatus}");
            currentQueue.Status = nameof(Constants.JobStatus.Complete);
            this.UpdateQueueWithDelete(new List<IFileItem> { currentQueue }, Constants.JobStatus.Complete, true);
        }
        else if (jobResult.JobStatus == ResultState.WAITING || jobResult.JobStatus == ResultState.QUEUED)
        {
            LogMessage(NLog.LogLevel.Info, $"FileGUID: {currentQueue.FileGUID}; Update queue status back to 'Pending' as job is not yet complete; jobRunID: {jobResult.JobRunID}. Job status: {jobResult.JobStatus}");
            currentQueue.Status = nameof(Constants.JobStatus.Pending);
            JobService.UpdateQueueStatus(jobResult.QueueID, Constants.JobStatus.Pending);
        }
        else
        {
            _exceptionCount++;
            CancelJobSubmissions();

            LogMessage(NLog.LogLevel.Error, $"ERROR->FileGUID: {currentQueue.FileGUID};QueueID: {jobResult.QueueID}; Spark job failed- JobRunID={jobResult.JobRunID} - JobID={_databricksEtlJob.DatabricksJobID}. Job status: {jobResult.JobStatus}");
            currentQueue.Status = nameof(Constants.JobStatus.Error);
            JobService.UpdateQueueStatus(jobResult.QueueID, Constants.JobStatus.Error, false);

            LogMessage(NLog.LogLevel.Error, $"ERROR->FileGUID: {currentQueue.FileGUID};deleting from Partition table fileguid entries.");
            JobService.DeletePartitionByFileGUID(currentQueue.FileGUID.ToString());
        }
    }

    private void LogMaxRunTimeWarning()
    {
        LogWarning($"Current runtime:{_runTime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
    }

    private bool IsMaxRunTimeReachedAndTaskCancelled(Exception ex)
    {
        return ex is OperationCanceledException && TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1;
    }

    #endregion

    #region pre execute helpers

    private DatabricksJobProvider CreateDatabricksJobProvider()
    {
        DatabricksETLJobRepository etlJobRepo = new();
        _databricksEtlJob = etlJobRepo.GetEtlJobBySourceID(CurrentSource.SourceID) ?? throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for SourceID=" + CurrentSource.SourceID);
        string encryptedConnectionString = LookupService.GetLookupValueWithDefault(Constants.DATABRICKS_API_CREDS) ?? throw new LookupException($"Lookup value for {Greenhouse.Common.Constants.DATABRICKS_API_CREDS} is not defined");
        Credential databricksCredential = new(encryptedConnectionString);
        int pageSize = LookupService.GetLookupValueWithDefault(Constants.DATABRICKS_API_PAGESIZE, 25);

        return new DatabricksJobProvider(
            new DatabricksJobProviderOptions
            {
                IntegrationID = CurrentIntegration.IntegrationID,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                MaxConcurrentJobs = LookupService.GetGlobalLookupValueWithDefault(Constants.TTD_DELIVERY_MAX_CONCURRENT_JOBS, CurrentIntegration.IntegrationID, 5),
                RetryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.TTD_DELIVERY_STATUS_CHECK_DELAY_SECONDS, 30),
                DatabricksJobID = _databricksEtlJob.DatabricksJobID,
                Logger = LogMessage,
                ExceptionLogger = LogException,
                JobRequestRetryMaxAttempts = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_MAX_RETRY, CurrentSource.SourceID, 3),
                JobRequestRetryDelayInSeconds = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_DELAY_SECONDS, CurrentSource.SourceID, 1),
                JobRequestRetryUseJitter = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_REQUESTS_BACKOFF_USE_JITTER, CurrentSource.SourceID, true),
                JobStatusCheckRetryMaxAttempts = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_MAX_RETRY, CurrentSource.SourceID, 3),
                JobStatusCheckRetryDelayInSeconds = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_DELAY_SECONDS, CurrentSource.SourceID, 1),
                JobStatusCheckRetryUseJitter = LookupService.GetGlobalLookupValueWithDefault(Constants.DATABRICKS_API_JOB_STATUS_BACKOFF_USE_JITTER, CurrentSource.SourceID, true)
            },
            new DatabricksCalls(databricksCredential, pageSize, HttpClientProvider),
            new DatabricksJobLogRepository()
            );
    }

    #endregion

    #region Logs

    private void LogMessage(LogLevel logLevel, string message)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
    }

    private void LogException(LogLevel logLevel, string message, Exception exc = null)
    {
        _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
    }

    private void LogWarning(string message)
    {
        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(message)));
        _warningCount++;
    }

    #endregion

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
            _cts.Dispose();
        }
    }

        ~DeliveryProcessing()
        {
            Dispose(false);
        }
    }

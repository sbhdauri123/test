using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Greenhouse.Jobs.DCM.Metadata
{
    [Export("DCM-MetadataDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private DatabricksJobProvider _databricksJobProvider;
        private DatabricksETLJob _databricksEtlJob;
        private readonly CancellationTokenSource _cts = new();
        private TimeSpan _maxRuntime;
        private int _exceptionCount;
        private int _warningCount;
        private readonly Stopwatch _runTime = new();
        private IFileItem _queueItem;
        private TimeSpan _dbxWaitTime;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);
            _databricksJobProvider = CreateDatabricksJobProvider();
            _cts.CancelAfter(_maxRuntime);
            _dbxWaitTime = LookupService.GetLookupValueWithDefault($"{Constants.DATABRICKS_FAILED_RUN_WAIT_TIME}{CurrentSource.SourceID}", new TimeSpan(0, 6, 0, 0));
        }

        public void Execute()
        {
            LogMessage(LogLevel.Info, $"EXECUTE START {base.DefaultJobCacheKey}");

            try
            {
                _runTime.Start();

                _queueItem = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).FirstOrDefault();
                if (_queueItem == null)
                {
                    LogMessage(LogLevel.Info, "No queue to process.");
                    _runTime.Stop();
                    return;
                }

                if (_queueItem.Status == Constants.JobStatus.Error.ToString() && _queueItem.LastUpdated + _dbxWaitTime > DateTime.Now)
                {
                    LogMessage(LogLevel.Info, $"Skipping processing of queue {_queueItem.ID} - Databricks job was recently submitted");
                    return;
                }
                MarkQueueRunning();

                Uri path = GetStageFilePath();

                ProcessDatabricksEtl(_queueItem, path);

                ProcessSqlEtlAndMarkQueueComplete(_queueItem, path);
            }
            catch (Exception exc)
            {
                if (IsMaxRunTimeReachedAndTaskCancelled(exc))
                {
                    LogMessage(LogLevel.Warn, $"Current runtime:{_runTime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
                }
                else
                {
                    LogException(LogLevel.Error, $"Error caught in Execute. Message:{exc.Message} - STACK {exc.StackTrace}", exc);
                    _exceptionCount++;
                    MarkQueueError();
                }
            }

            _runTime.Stop();

            if (_exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
            }

            if (_warningCount > 0)
            {
                JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
                JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
            }

            LogMessage(LogLevel.Info, $"EXECUTE END {base.DefaultJobCacheKey}");
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
                _cts.Dispose();
            }
        }

        ~DataLoadJob()
        {
            Dispose(false);
        }

        #region execute
        private void ProcessSqlEtlAndMarkQueueComplete(IFileItem queueItem, Uri path)
        {
            if (_exceptionCount > 0 || _warningCount > 0)
            {
                LogMessage(LogLevel.Info, $"Job in Databricks is not complete. Skipping load into sql Dimension database - Job ID: {_databricksEtlJob.DatabricksJobID}");
                return;
            }

            LogMessage(LogLevel.Info, $"Start loading DCM Metadata into SQL Dimension DB file->path:{path};guid:{queueItem.FileGUID};");

            string jobGUID = this.JED.JobGUID.ToString();
            var etl = new Greenhouse.DAL.ETLProvider();
            etl.SetJobLogGUID(jobGUID);

            etl.LoadDCMMetadata(queueItem.FileGUID, path, base.SourceId, base.IntegrationId, CurrentIntegration.CountryID, queueItem.FileCollection.ToList());

            LogMessage(LogLevel.Info, "End loading DCM Metadata into SQL Dimension DB");

            MarkQueueComplete();
        }

        private void ProcessDatabricksEtl(IFileItem queueItem, Uri path)
        {
            LogMessage(LogLevel.Info, $"Start loading DCM Metadata file into Databricks->path:{path};guid:{queueItem.FileGUID};");

            string stageFilePath = System.Net.WebUtility.UrlDecode($"{path.ToString().Trim('/')}");

            JobRunRequest jobRequest = new()
            {
                JobID = Convert.ToInt64(_databricksEtlJob.DatabricksJobID),
                JobParameters = _databricksJobProvider.CreateStandardizedJobParameters(
                    new DatabricksJobParameterOptions()
                    {
                        StageFilePath = stageFilePath,
                        FileGuid = queueItem.FileGUID.ToString(),
                        FileDate = queueItem.FileDate.ToString(CurrentSource.AggregateProcessingSettings.FileDateFormat ?? "MM-dd-yyyy"),
                        EntityID = queueItem.EntityID
                    })
            };

            _databricksJobProvider.QueueJobAsync(queueItem.ID, jobRequest, (queueID, result) => OnJobException(queueID), _cts.Token).GetAwaiter().GetResult();
            _databricksJobProvider.WaitForMaxRunJobsToCompleteAsync(OnJobCompletion, (queueID, result) => OnJobException(queueID), _cts.Token, true).GetAwaiter().GetResult();

            LogMessage(LogLevel.Info, "End loading DCM Metadata into Databricks");
        }

        private bool IsMaxRunTimeReachedAndTaskCancelled(Exception ex)
        {
            return ex is OperationCanceledException && TimeSpan.Compare(_runTime.Elapsed, _maxRuntime) == 1;
        }

        private Uri GetStageFilePath()
        {
            var baseDestUri = base.GetDestinationFolder();
            string[] paths = [_queueItem.EntityID.ToLower(), GetDatedPartition(_queueItem.FileDate)];
            Uri path = RemoteUri.CombineUri(baseDestUri, paths);
            return path;
        }
        #endregion

        #region DBX
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
                    MaxConcurrentJobs = LookupService.GetGlobalLookupValueWithDefault(Constants.DSP_DATALOAD_MAX_CONCURRENT_JOBS, CurrentIntegration.IntegrationID, 5),
                    RetryDelayInSeconds = LookupService.GetLookupValueWithDefault(Constants.DSP_DATALOAD_STATUS_CHECK_DELAY_SECONDS, 30),
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

        private void OnJobException(long queueID)
        {
            _exceptionCount++;
            LogMessage(LogLevel.Error, $"ERROR->FileGUID: {_queueItem.FileGUID} Something went wrong with the Databricks workflow for DCM Metadata Job ID: {_databricksEtlJob.DatabricksJobID} - Queue ID: {queueID}");
            MarkQueueError();
        }

        private void OnJobCompletion(DatabricksJobResult jobResult)
        {
            if (jobResult.JobStatus == ResultState.SUCCESS)
            {
                LogMessage(NLog.LogLevel.Info, $"SUCCESS->FileGUID: {_queueItem.FileGUID};QueueID: {jobResult.QueueID}; Databricks job completed - JobRunID={jobResult.JobRunID} - JobID={_databricksEtlJob.DatabricksJobID}. Job status: {jobResult.JobStatus}");
            }
            else if (jobResult.JobStatus == ResultState.WAITING || jobResult.JobStatus == ResultState.QUEUED)
            {
                _warningCount++;
                LogMessage(NLog.LogLevel.Warn, $"FileGUID: {_queueItem.FileGUID}; Databricks job is not yet complete; jobRunID: {jobResult.JobRunID}. Job status: {jobResult.JobStatus}");
                MarkQueuePending();
            }
            else
            {
                _exceptionCount++;
                LogMessage(NLog.LogLevel.Error, $"ERROR->FileGUID: {_queueItem.FileGUID};QueueID: {jobResult.QueueID}; Databricks job failed- JobRunID={jobResult.JobRunID} - JobID={_databricksEtlJob.DatabricksJobID}. Job status: {jobResult.JobStatus}");
                MarkQueueError();
            }
        }

        #endregion

        #region Queue status
        private void MarkQueueError()
        {
            _queueItem.Status = nameof(Constants.JobStatus.Error);
            JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Error, false);
        }

        private void MarkQueueComplete()
        {
            LogMessage(LogLevel.Info, "Marking Queue as Complete");
            _queueItem.Status = nameof(Constants.JobStatus.Complete);
            this.UpdateQueueWithDelete(new List<IFileItem> { _queueItem }, Constants.JobStatus.Complete, true);
        }

        private void MarkQueuePending()
        {
            _queueItem.Status = nameof(Constants.JobStatus.Pending);
            JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Pending, false);
        }

        private void MarkQueueRunning()
        {
            _queueItem.Status = nameof(Constants.JobStatus.Running);
            JobService.UpdateQueueStatus(_queueItem.ID, Constants.JobStatus.Running);
        }
        #endregion

        #region Logging

        private void LogMessage(LogLevel logLevel, string message)
        {
            _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message)));
        }

        private void LogException(LogLevel logLevel, string message, Exception exc = null)
        {
            _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(message), exc));
        }

        #endregion
    }
}

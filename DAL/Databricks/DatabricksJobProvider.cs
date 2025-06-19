using Greenhouse.DAL.Databricks.RunListResponse;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.DAL.Databricks;

public class DatabricksJobProvider : IDatabricksJobProvider
{
    private readonly IDatabricksJobLogRepository _databricksJobLogRepo;
    private readonly IDatabricksCalls _api;
    private readonly DatabricksJobProviderOptions _options;
    private readonly ResiliencePipeline _jobRequestPollyRetry;
    private readonly ResiliencePipeline _jobStatusPollyRetry;

    #region Workflow standardized parameter names

    private const string JOB_PARAMETER_KEY_STAGEFILEPATH = "stagefilepath";
    private const string JOB_PARAMETER_KEY_FILEGUID = "fileguid";
    private const string JOB_PARAMETER_KEY_FILEDATE = "filedate";
    private const string JOB_PARAMETER_KEY_ENTITYID = "entityid";
    private const string JOB_PARAMETER_KEY_ISDIMENSION = "isDimension";
    private const string JOB_PARAMETER_KEY_ENTITYNAME = "entityname";
    private const string JOB_PARAMETER_KEY_PROFILEID = "profileid";
    private const string JOB_PARAMETER_KEY_PROFILENAME = "profilename";
    private const string JOB_PARAMETER_KEY_MANIFEST = "manifest";
    private const string JOB_PARAMETER_KEY_SOURCEFILE_PREFIX = "sourcefile-";
    private const string JOB_PARAMETER_KEY_NOOFCONCURRENTPROCESSES = "noofconcurrentprocesses";
    private const string JOB_PARAMETER_KEY_SOURCEID = "sourceid";

    #endregion

    public List<DatabricksJobResult> RunningJobs { get; } = new();

    public int MaxConcurrentJobs { get; }

    public DatabricksJobProvider(DatabricksJobProviderOptions options, IDatabricksCalls databricksApiClient, IDatabricksJobLogRepository databricksJobLogRepo)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(databricksApiClient);
        ArgumentNullException.ThrowIfNull(databricksJobLogRepo);

        options.Validate();

        _api = databricksApiClient;
        _databricksJobLogRepo = databricksJobLogRepo;
        _options = options;
        MaxConcurrentJobs = options.MaxConcurrentJobs;
        _jobRequestPollyRetry = GetResiliencePipeline(options.JobRequestRetryMaxAttempts, options.JobRequestRetryDelayInSeconds, options.JobRequestRetryUseJitter);
        _jobStatusPollyRetry = GetResiliencePipeline(options.JobStatusCheckRetryMaxAttempts, options.JobStatusCheckRetryDelayInSeconds, options.JobStatusCheckRetryUseJitter);
    }

    public void InitializeRunningJobs(List<long> queueIDs)
    {
        if (queueIDs.Count == 0)
        {
            return;
        }

        foreach (var queueID in queueIDs)
        {
            if (RunningJobs.Exists(x => x.QueueID == queueID))
            {
                continue;
            }

            DatabricksJobLog latestJobLog = GetLatestJobLog(queueID);
            bool isJobRunning = IsJobRunningOrComplete(latestJobLog);
            if (isJobRunning)
            {
                DatabricksJobResult result = new()
                {
                    QueueID = queueID,
                    JobRunID = latestJobLog.RunID
                };

                RunningJobs.Add(result);
            }
        }
    }

    /// <summary>
    /// Polly Retry Strategy - 429 Errors are retried.
    /// </summary>
    private ResiliencePipeline GetResiliencePipeline(int maxRetry, int retryDelayInSeconds, bool retryUseJitter)
    {
        var optionsOnRetry = new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>(httpRequestException => httpRequestException.StatusCode == HttpStatusCode.TooManyRequests),
            BackoffType = DelayBackoffType.Constant,
            UseJitter = retryUseJitter,
            Delay = TimeSpan.FromSeconds(retryDelayInSeconds),
            MaxRetryAttempts = maxRetry,
            OnRetry = args =>
            {
                _options.ExceptionLogger(LogLevel.Warn, $"Polly-OnRetry with Exception: {args.Outcome.Exception.Message}. Backoff Policy retry attempt: {args.AttemptNumber}", args.Outcome.Exception);
                return default;
            }
        };

        return new ResiliencePipelineBuilder()
            .AddRetry(optionsOnRetry)
            .Build();
    }

    private List<DatabricksJobLog> _databricksJobLogs;

    private DatabricksJobLog GetLatestJobLog(long queueID)
    {
        _databricksJobLogs ??= _databricksJobLogRepo.GetDatabricksJobLogs(_options.IntegrationID, _options.JobLogID).ToList();
        return _databricksJobLogs.Find(x => x.QueueID == queueID);
    }

    /// <summary>
    /// Return TRUE if status is WAITING/QUEUED/SUCCESS
    /// </summary>
    private static bool IsJobRunningOrComplete(DatabricksJobLog latestJobLog)
    {
        bool isRunningOrComplete = false;

        if (latestJobLog == null)
        {
            return false;
        }

        var runStatus = Utilities.UtilsText.ConvertToEnum<ResultState>(latestJobLog.Status);
        if (runStatus == ResultState.WAITING || runStatus == ResultState.SUCCESS || runStatus == ResultState.QUEUED)
        {
            isRunningOrComplete = true;
        }

        return isRunningOrComplete;
    }

    public Dictionary<string, string> CreateStandardizedJobParameters(DatabricksJobParameterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        Dictionary<string, string> parameters = new()
        {
            { JOB_PARAMETER_KEY_STAGEFILEPATH, options.StageFilePath },
            { JOB_PARAMETER_KEY_FILEGUID, options.FileGuid},
            { JOB_PARAMETER_KEY_FILEDATE, options.FileDate},
            { JOB_PARAMETER_KEY_ISDIMENSION, options.IsDimOnly.ToString().ToLower()}
        };

        if (!string.IsNullOrEmpty(options.EntityID))
        {
            parameters.Add(JOB_PARAMETER_KEY_ENTITYID, options.EntityID);
        }

        if (!string.IsNullOrEmpty(options.EntityName))
        {
            parameters.Add(JOB_PARAMETER_KEY_ENTITYNAME, options.EntityName);
        }

        if (!string.IsNullOrEmpty(options.Profileid))
        {
            parameters.Add(JOB_PARAMETER_KEY_PROFILEID, options.Profileid);
        }

        if (!string.IsNullOrEmpty(options.ProfileName))
        {
            parameters.Add(JOB_PARAMETER_KEY_PROFILENAME, options.ProfileName);
        }

        if (!string.IsNullOrEmpty(options.ManifestFilePath))
        {
            parameters.Add(JOB_PARAMETER_KEY_MANIFEST, options.ManifestFilePath);
        }

        if (!string.IsNullOrEmpty(options.FileCollectionJson))
        {
            foreach (var keyValuePair in ETLProvider.GetStageFileDictionary(options.FileCollectionJson))
            {
                if (!parameters.ContainsKey(keyValuePair.Key))
                {
                    parameters.Add($"{JOB_PARAMETER_KEY_SOURCEFILE_PREFIX}{keyValuePair.Key}", keyValuePair.Value);
                }
            }
        }
        if (options.NoOfConcurrentProcesses.HasValue)
        {
            parameters.Add(JOB_PARAMETER_KEY_NOOFCONCURRENTPROCESSES, options.NoOfConcurrentProcesses.ToString());
        }
        if (options.SourceId.HasValue)
        {
            parameters.Add(JOB_PARAMETER_KEY_SOURCEID, options.SourceId.ToString());
        }
        return parameters;
    }

        /// <summary>
        /// Submits single Databricks job request and adds to a tracking list that will be used for status-checks.
        /// If previously submitted, then existing job run ID will be added to tracking list.
        /// Caller has delegate to handle error exception is caught.
        /// </summary>
        /// <returns></returns>
        public async Task QueueJobAsync(long queueID, JobRunRequest jobRequest, Action<long, long> onException, CancellationToken cancellationToken)
        {
            DatabricksJobResult result = new() { QueueID = queueID };

        try
        {
            result.JobRunID = await SubmitJobAsync(queueID, jobRequest, cancellationToken);

            _options.Logger(LogLevel.Debug, $"Databricks job submitted ->QueueID: {queueID}; JobRunID={result.JobRunID} - JobID={jobRequest.JobID}. Request:{JsonConvert.SerializeObject(jobRequest)}");

            RunningJobs.Add(result);

            await Task.Delay(1000, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            RunningJobs.Remove(result);
        }
        catch (HttpClientProviderRequestException exception)
        {
            HandleException(queueID, jobRequest, onException, result, exception);
        }
        catch (Exception ex)
        {
            HandleException(queueID, jobRequest, onException, result, ex);
        }
    }

        private void HandleException<TException>(long queueID, JobRunRequest jobRequest, 
            Action<long, long> onException, DatabricksJobResult result,
            TException ex) where TException : Exception
        {
            // Build log message
            var logMessage = BuildLogMessage(queueID, jobRequest, result, ex);

        // Log the exception
        _options.ExceptionLogger(LogLevel.Error, logMessage, ex);

            // Invoke the exception callback
            onException(queueID, result.JobRunID);
        }

        private string BuildLogMessage<TException>(
            long queueID,
            JobRunRequest jobRequest,
            DatabricksJobResult result,
            TException ex) where TException : Exception
        {
            return ex switch
            {
                HttpClientProviderRequestException httpEx =>
                    $"Error in {nameof(DatabricksJobProvider)}.{nameof(QueueJobAsync)} -> " +
                    $"QueueID: {queueID}; " +
                    $"JobRunID={result?.JobRunID} - " +
                    $"JobID={jobRequest?.JobID}. " +
                    $"Request: {JsonConvert.SerializeObject(jobRequest)} - " +
                    $"Exception details: {httpEx}",
                _ =>
                    $"Exception in SubmitJobAsync - ErrorMessage: {ex.Message} -> " +
                    $"QueueID: {queueID}; " +
                    $"JobRunID={result?.JobRunID} - " +
                    $"JobID={jobRequest?.JobID}. " +
                    $"Request: {JsonConvert.SerializeObject(jobRequest)} - " +
                    $"Stack: {ex.StackTrace}"
            };
        }
        /// <summary>
        /// Only checks status when max concurrent jobs is reached. Status check ends when any job returns a done status.
        /// Pass checkAllJobsNow as TRUE if you want to wait for all jobs to finish checking their status, ie submitted job for last queue item.
        /// </summary>
        public async Task WaitForMaxRunJobsToCompleteAsync(Action<DatabricksJobResult> onJobCompletion, Action<long, long> onException, CancellationToken cancellationToken, bool checkAllJobsNow = false)
        {
            if (RunningJobs.Count < MaxConcurrentJobs && !checkAllJobsNow)
            {
                return;
            }

        if (RunningJobs.Count == 0)
        {
            return;
        }

            bool isDone = false;
            int finishedCounter = 0;
            do
            {
                foreach (var runningJob in RunningJobs.ToList())
                {
                    try
                    {
                        bool isFinished = await ConfirmJobFinishedAsync(onJobCompletion, runningJob, cancellationToken);
                        if (isFinished)
                        {
                            finishedCounter++;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        RunningJobs.Remove(runningJob);
                        break;
                    }
                    catch (DbException ex)
                    {
                        RunningJobs.Remove(runningJob);
                        _options.ExceptionLogger(NLog.LogLevel.Error, $"Database Exception in WaitForMaxRunJobsToCompleteAsync-ErrorMessage:{ex.Message}->QueueID: {runningJob.QueueID}; JobRunID={runningJob.JobRunID} - JobID={_options.DatabricksJobID} - STACK {ex.StackTrace}", ex);
                        onException(runningJob.QueueID, runningJob.JobRunID);
                    }
                    catch (Exception ex)
                    {
                        RunningJobs.Remove(runningJob);
                        _databricksJobLogRepo.UpdateDatabricksJobLog(runningJob.QueueID, runningJob.JobRunID, nameof(ResultState.FAILED), Convert.ToInt64(_options.DatabricksJobID));
                        _options.ExceptionLogger(NLog.LogLevel.Error, $"Exception in WaitForMaxRunJobsToCompleteAsync-ErrorMessage:{ex.Message}->QueueID: {runningJob.QueueID}; JobRunID={runningJob.JobRunID} - JobID={_options.DatabricksJobID} - STACK {ex.StackTrace}", ex);
                        onException(runningJob.QueueID, runningJob.JobRunID);
                    }
                }

            if (cancellationToken.IsCancellationRequested || RunningJobs.Count == 0)
            {
                break;
            }

            if (!checkAllJobsNow && finishedCounter > 0 && RunningJobs.Count < MaxConcurrentJobs)
            {
                isDone = true;
            }

            if (!isDone)
            {
                await DelayBetweenStatusChecksAsync(cancellationToken);
            }

            _options.Logger(LogLevel.Debug, $"Databricks Job Status Check Progress => Total running jobs:{RunningJobs.Count} - Total finished:{finishedCounter} at {DateTime.Now}");
        } while (!isDone);
    }

    private async Task DelayBetweenStatusChecksAsync(CancellationToken cancellationToken)
    {
        int delayInMilliseconds = 1000 * _options.RetryDelayInSeconds;
        await Task.Delay(delayInMilliseconds, cancellationToken);
    }

    /// <summary>
    /// Checks status of a single Job Run ID - finished status is any of the following: SUCCESS/FAILED/CANCELED/SKIPPED
    /// </summary>
    private async Task<bool> ConfirmJobFinishedAsync(Action<DatabricksJobResult> onJobCompletion, DatabricksJobResult runningJob, CancellationToken cancellationToken)
    {
        bool isDone = false;

        List<ResultState> doneSteps = new()
        {
            ResultState.SUCCESS,
            ResultState.FAILED,
            ResultState.CANCELED,
            ResultState.SKIPPED
        };

        runningJob.JobStatus = await _jobStatusPollyRetry.ExecuteAsync(async (_) => await _api.CheckJobStatusAsync(runningJob.JobRunID), cancellationToken);

        if (doneSteps.Contains(runningJob.JobStatus))
        {
            _databricksJobLogRepo.UpdateDatabricksJobLog(runningJob.QueueID, runningJob.JobRunID, runningJob.JobStatus.ToString(), Convert.ToInt64(_options.DatabricksJobID));
            RunningJobs.Remove(runningJob);
            // allow caller to take any action post-job completion
            onJobCompletion(runningJob);
            isDone = true;
        }

        await Task.Delay(1000, cancellationToken);

        return isDone;
    }

    /// <summary>
    /// Returns latest run ID if latest job is still pending/complete, otherwise will submit new job for new run ID
    /// </summary>
    private async Task<long> SubmitJobAsync(long queueID, JobRunRequest jobRequest, CancellationToken cancellationToken)
    {
        DatabricksJobLog latestJobLog = GetLatestJobLog(queueID);
        bool isJobRunning = IsJobRunningOrComplete(latestJobLog);
        if (isJobRunning)
        {
            return latestJobLog.RunID;
        }

        JobRunResponse response = await _jobRequestPollyRetry.ExecuteAsync(async (_) => await _api.RunJobAsync(jobRequest), cancellationToken);

        long runID = response.RunID;
        _options.Logger(LogLevel.Info, $"Databricks run submitted for Queue ID:{queueID}-jobID:{jobRequest.JobID}; JobRun ID:{runID};");

        _databricksJobLogRepo.UpdateDatabricksJobLog(queueID, runID, nameof(ResultState.WAITING), jobRequest.JobID, JsonConvert.SerializeObject(jobRequest));

        return runID;
    }

    public async Task WaitForJobToCompleteAsync(long queueID, Action<DatabricksJobResult> onJobCompletion, Action<long> onException, CancellationToken cancellationToken)
    {
        if (!RunningJobs.Exists(x => x.QueueID == queueID))
        {
            return;
        }

        bool isDone = false;
        int counter = 0;

        do
        {
            counter++;
            var runningJob = RunningJobs.Find(x => x.QueueID == queueID);

            try
            {
                isDone = await ConfirmJobFinishedAsync(onJobCompletion, runningJob, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                RunningJobs.Remove(runningJob);
                break;
            }
            catch (DbException ex)
            {
                RunningJobs.Remove(runningJob);
                _options.ExceptionLogger(NLog.LogLevel.Error, $"Database Exception in WaitForJobToCompleteAsync-ErrorMessage:{ex.Message}->QueueID: {runningJob.QueueID}; JobRunID={runningJob.JobRunID} - JobID={_options.DatabricksJobID} - STACK {ex.StackTrace}", ex);
                onException(runningJob.QueueID);
                break;
            }
            catch (Exception ex)
            {
                RunningJobs.Remove(runningJob);
                _databricksJobLogRepo.UpdateDatabricksJobLog(runningJob.QueueID, runningJob.JobRunID, nameof(ResultState.FAILED), Convert.ToInt64(_options.DatabricksJobID));
                _options.ExceptionLogger(NLog.LogLevel.Error, $"Exception in WaitForJobToCompleteAsync-ErrorMessage:{ex.Message}->QueueID: {runningJob.QueueID}; JobRunID={runningJob.JobRunID} - JobID={_options.DatabricksJobID} - STACK {ex.StackTrace}", ex);
                onException(runningJob.QueueID);
                break;
            }

            if (!isDone)
            {
                await DelayBetweenStatusChecksAsync(cancellationToken);
            }

            _options.Logger(LogLevel.Debug, $"Databricks Job Status Check Progress => Queue ID: {queueID} - Total trips:{counter} at {DateTime.Now}");
        } while (!isDone);
    }
}

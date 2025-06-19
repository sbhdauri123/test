using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Contracts.Messages;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.Jobs.Infrastructure
{
    public class ProcessingJob : BaseQuartzJob, IJob
    {
        private readonly NLog.ILogger _logger;
        private readonly Data.Model.Setup.Server _setupServer;
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly ITokenCache _tokenCache;
        private readonly IJobExecutionHandler _jobExecutionHandler;

        // Quartz job factory requires an empty constructor
        public ProcessingJob()
        {
        }

        public ProcessingJob(NLog.ILogger logger, IJobLoggerFactory jobLoggerFactory,
            ISchedulerFactory schedulerFactory,
            ICacheStore cache, Data.Model.Setup.Server setupServer,
            IHttpClientProvider httpClientProvider, ITokenCache tokenCache, IJobExecutionHandler jobExecutionHandler) : base(jobLoggerFactory, schedulerFactory,
            cache)
        {
            _logger = logger;
            _setupServer = setupServer;
            _httpClientProvider = httpClientProvider;
            _tokenCache = tokenCache;
            _jobExecutionHandler = jobExecutionHandler;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            Scheduler = await SchedulerFactory.GetScheduler();
            JobExecutionDetails JED = null;
            bool canAutoRetry = false;
            bool isDuplicateJob = false;

            try
            {
                //The underlying job's execution details
                JED = Newtonsoft.Json.JsonConvert.DeserializeObject<JobExecutionDetails>(context.MergedJobDataMap.GetString(Constants.JOB_EXECUTION_DETAILS));

                ExecuteJob executeJob = _jobExecutionHandler.CreateExecuteJob(JED);
                if (executeJob != null && await _jobExecutionHandler.TryPublishJobExecutionMessage(executeJob))
                {
                    return;
                }

                _logger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Info, _logger.Name, string.Format("ProcessingJob - ContractKey: {0} SourceStep: {1} JobName: {2}", JED.ContractKey, JED.ExecutionPath.CurrentStep == null ? "NULL" : JED.ExecutionPath.CurrentStep.SourceJobStepName, JED.ProcessingJobName)));
                //Property ContractKey is DataSource + JobStep (e.g. BrightEdgeQuery)                
                ContractKey = JED.ContractKey;
                //Satisfy import by finding exported part(s)
                // Inject services to BaseQuartzJob and BaseDragoJob
                ComposeFromSelf(Scheduler, JobLoggerFactory, Cache, _logger, _setupServer);
                BaseDragoJob.HttpClientProvider = _httpClientProvider;
                BaseDragoJob.TokenCache = _tokenCache;

                context.JobDetail.JobDataMap[Constants.JOB_EXECUTION_DETAILS] = JED.ToJSON();
                //Unload Context to Job                               
                BaseDragoJob.InitFromExecutionContext(context);
                //Initialize the JobLogger
                //IoC: Initialize dependency services    

                Dictionary<string, object> jobDataMap = new Dictionary<string, object>();
                jobDataMap[Constants.JOB_EXECUTION_DETAILS] = JED;
                jobDataMap[Constants.JOB_LOG] = Newtonsoft.Json.JsonConvert.DeserializeObject<JobLog>(context.MergedJobDataMap.GetString(Constants.JOB_LOG));

                BaseDragoJob.JobLogger.Initialize(jobDataMap);

                // **WARNING**
                // Following job cache key check is too strict Backfill scheduling
                // EX: If BF-job-1 and BF-job-2 are scheduled to run at same time, then one of them will be blocked
                // TODO: 1) Add UI validation &
                // TODO: 2) retry after 15 minutes - for backfill only
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, $"{JED.JobGUID}-CheckingCacheContents|JobGroup={JED.JobGroup}|" +
                    $"JobName={JED.JobName}|JobCacheKey={JED.JobCacheKey}|ExistingKeys={String.Join(",", CacheStore.GetKeys())}"));

                isDuplicateJob = CacheStore.Exists(JED.JobCacheKey);
                if (isDuplicateJob)
                {
                    var message = $"duplicate job trying to run, but JED {JED.JobCacheKey} exists in cache";
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, $"{JED.JobGUID} - ProcessingJob - {message}"));
                    BaseDragoJob.JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                    BaseDragoJob.JobLogger.JobLog.Message = message;
                    BaseDragoJob.JobLogger.Finish();
                }
                else
                {
                    // add cache key here after the jobkey is checked and before the job logger is started
                    CacheStore.Set<string>(JED.JobCacheKey, "1", new TimeSpan(3650, 0, 0, 0));

                    Trace("calling BaseDragoJob.Started()");
                    //kicks off the start timer for this step and saves the record off to the JobLogs
                    BaseDragoJob.JobLogger.Start();
                    Trace("BaseDragoJob.Started() complete");

                    //add the jobkey to cache (it gets removed in finally block so it's always pruged regardless of errors)
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, string.Format("SETTING JobCacheKey: {0}", JED.JobCacheKey)));

                    // logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("ProcessingJob - JobCacheKey: {0} Existing Keys: {1}", job.GetJobCacheKey(), CacheStore.GetKeys().DumpToString())));

                    //pre-execute initializes the job
                    Trace("Calling PreExecute()");
                    job.PreExecute();
                    Trace("PreExecute() complete");

                    //the main bulk of the job's core processing
                    Trace("calling Execute()");
                    job.Execute();
                    Trace("Execute() complete");

                    Trace("calling BaseDragoJob.Finished()");

                    BaseDragoJob.JobLogger.Finish();

                    Trace("BaseDragoJob.Finished() complete");

                    Trace("calling PostExecute()");
                    job.PostExecute();
                    Trace("PostExecute() complete");
                }
            }
            catch (Exception e)
            {
                if (BaseDragoJob?.JobLogger is not null)
                {
                    BaseDragoJob.JobLogger.LogException(e);
                }
                else
                {
                    _logger.Error(e);
                }

                Trace("BaseDragoJob.LogException() complete");

                canAutoRetry = true;
            }
            finally
            {
                if (BaseDragoJob is not null)
                {
                    BaseDragoJob.Complete();

                    if (!isDuplicateJob)
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, $"REMOVING JobCacheKey: {JED.JobCacheKey}"));

                        //always remove from cache whether success or failure
                        this.CacheStore.Remove(JED.JobCacheKey);
                    }

                    if (canAutoRetry)
                    {
                        BaseDragoJob.RetryJob();
                    }
                }

                if (job != null)
                {
                    Trace("Calling job.Dispose()");
                    job.Dispose();
                    Trace("job.Dispose() complete");
                }
            }
        }
    }
}

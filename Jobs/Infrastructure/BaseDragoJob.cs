using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure
{
    public class BaseDragoJob
    {
        protected string Environment = string.Empty;
        private IJobExecutionContext _context;
        private Greenhouse.Data.Model.Setup.Server _setupServer;

        protected int filesIn;
        protected int filesTrans;
        protected int filesOut;
        protected long bytesIn;
        protected long bytesOut;

        public Greenhouse.Logging.IJobLogger JobLogger { get; set; }
        public Greenhouse.Caching.ICacheStore CacheStore { get; set; }
        public Quartz.IScheduler Scheduler { get; set; }
        public NLog.ILogger Logger { get; set; }

        #region Init and User Selection

        public void InitFromExecutionContext(IJobExecutionContext context)
        {
            this.Environment = Greenhouse.Configuration.Settings.Current.Application.Environment;
            this._context = context;
            this.JED = Newtonsoft.Json.JsonConvert.DeserializeObject<JobExecutionDetails>(context.MergedJobDataMap.GetString(Constants.JOB_EXECUTION_DETAILS));
        }

        public async Task Initialize(int sourceId, int integrationId, int serverId)
        {
            JobExecutionDetails jobExecutionDetails = new(
                cal: null,
                source: Data.Services.SetupService.GetById<Source>(sourceId),
                server: Data.Services.SetupService.GetById<Server>(serverId));

            jobExecutionDetails.JobProperties.Add("US_SOURCE_ID", sourceId);
            jobExecutionDetails.JobProperties.Add("US_INTEGRATION_ID", integrationId);

            StdSchedulerFactory factory = new StdSchedulerFactory();
            IScheduler scheduler = await factory.GetScheduler();
            IJobDetail jobDetail = JobBuilder.Create<NoOpJob>()
                .Build();

            jobDetail.JobDataMap.Put(Constants.JOB_EXECUTION_DETAILS, jobExecutionDetails);

            ITrigger trigger = TriggerBuilder.Create()
                .ForJob(jobDetail)
                .Build();

            NoOpJob job = new();
            JobExecutionContextImpl context = new(
                scheduler,
                new TriggerFiredBundle(
                    jobDetail,
                    (IOperableTrigger)trigger,
                    null,
                    false,
                    DateTimeOffset.UtcNow,
                    trigger.GetPreviousFireTimeUtc(),
                    trigger.GetNextFireTimeUtc(),
                    null
                ),
                job);

            context.MergedJobDataMap.Put(Constants.JOB_EXECUTION_DETAILS, jobExecutionDetails.ToJSON());
            JobLog jobLog = new() { JobData = jobExecutionDetails };
            context.MergedJobDataMap.Put(Constants.JOB_LOG,
                JsonConvert.SerializeObject(jobLog,
                    new JsonSerializerSettings
                    {
                        MissingMemberHandling = MissingMemberHandling.Ignore,
                        NullValueHandling = NullValueHandling.Ignore
                    }));

            InitFromExecutionContext(context);

            Dictionary<string, object> jobDataMap = new()
            {
                [Constants.JOB_EXECUTION_DETAILS] = JED,
                [Constants.JOB_LOG] = jobLog
            };

            JobLogger.Initialize(jobDataMap);
        }

        protected object GetUserSelection(string key)
        {
            System.Collections.IDictionaryEnumerator dicEnumerator = JED.JobProperties.GetEnumerator();
            object value = null;
            while (dicEnumerator.MoveNext())
            {
                string currKey = dicEnumerator.Key.ToString();
                if (currKey == key)
                {
                    value = dicEnumerator.Value;
                    break;
                }
            }
            return value;
        }

        #endregion

        #region Common Helper Objects/Methods

        public void SetCurrentServer(Greenhouse.Data.Model.Setup.Server server)
        {
            _setupServer = server;
        }

        protected Greenhouse.Data.Model.Setup.Server CurrentServer
        {
            get
            {
                return _setupServer;
            }
        }

        protected string QuartzJobName
        {
            get
            {
                if (this._context != null)
                {
                    return this._context.JobDetail.Key.Name;
                }
                return null;
            }
        }

        protected string QuartzJobGroup
        {
            get
            {
                if (this._context != null)
                {
                    return this._context.JobDetail.Key.Group;
                }
                return null;
            }
        }

        protected JobExecutionDetails JED { get; set; }

        protected JobExecutionDetails CloneJED()
        {
            // Quartz v3 uses JSON serialization
            var jed = Greenhouse.Utilities.UtilsIO.DeepCloneJson<JobExecutionDetails>(this.JED);

            return jed;
        }

        internal void LogException(Exception exc)
        {
            this.JobLogger.LogException(exc);
        }

        public void Started()
        {
            this.JobLogger.Start();
        }

        /// <summary>
        /// Executes when the job has completely finished regardless of it's outcome (failed/errored, exception, pending, etc)
        /// </summary>
        public void Complete()
        {
            //make sure the context has a reference to the latest, greatest JobLog
            _context.JobDetail.JobDataMap[Constants.JOB_LOG] = Newtonsoft.Json.JsonConvert.SerializeObject(this.JobLogger.JobLog);
        }

        public static void Finished()
        {
        }

        public string FullyQualifiedJobName
        {
            get
            {
                return this.GetType().FullName;
            }
        }

        public IHttpClientProvider HttpClientProvider { get; set; }

        public ITokenCache TokenCache { get; set; }

        public void RescheduleExistingJob(int additionalMinutes)
        {
            string msg = "Rescheduling existing job: {0} for {1} minutes";
            Logger.Log(Msg.Create(LogLevel.Trace, Logger.Name, string.Format(msg, JED.JobName, additionalMinutes)));

            JED.DelayedExecutionMins = additionalMinutes;

            ITrigger trigger = Infrastructure.JobAndTriggerBuilder.CreateTrigger(JED, true);
            // Quartz v3 uses JSON serialization
            JobLog newJobLog = Utilities.UtilsIO.DeepCloneJson<JobLog>(this.JobLogger.JobLog);

            trigger.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(newJobLog));

            IJobDetail job = Infrastructure.JobAndTriggerBuilder.CreateJob(JED, this.Scheduler, true);
            //make sure job name is completely unique
            string jobName = string.Format("{0}_RS{1}_{2}", job.Key.Name, JED.FailureCount, trigger.StartTimeUtc.Ticks);
            Logger.Log(Msg.Create(LogLevel.Trace, Logger.Name, string.Format("Job: {0} Scheduling New Job with: {1} PreviousFire: {2} NewFire: {3}", job.Key.Name, trigger.Key.Name, DateTime.Now, trigger.StartTimeUtc)));

            Scheduler.ScheduleJob(job, trigger);
        }

        public void RetryJob()
        {
            var sourceJob = this.JED.ExecutionPath.SourceJobSteps.SingleOrDefault(sj => sj.SourceJobStepID == JED.ExecutionPath.CurrentStep.SourceJobStepID);

            if (sourceJob.AutoRetryCount == 0)
            {
                string msg = string.Format("Job step: {0} - {1} does not have an AutoRetryCount defined, this job will NOT be automatically retried.", JED.ExecutionPath.CurrentStep.SourceJobStepName, this.FullyQualifiedJobName);
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name, msg));
                return;
            }
            //get in-memory count of how many retries it has already attempted
            string retryCacheKey = string.Format("RETRY_COUNT:{0}", JED.JobGUID);
            int numberOfRetries = CacheStore.Exists(retryCacheKey) ? CacheStore.Get<int>(retryCacheKey) : 0;
            //we can retry
            if (numberOfRetries < sourceJob.AutoRetryCount)
            {
                JED.IsRetry = true;
                numberOfRetries++;
                CacheStore.Set<int>(retryCacheKey, numberOfRetries, new TimeSpan(3650, 0, 0, 0));
                RescheduleExistingJob(sourceJob.DeferMinutes);
                string msg = string.Format("Job step: {0} - {1} has been rescheduled - Defer mins: {2}, currentretrycount < autoretrycount ({3} < {4})",
                    JED.ExecutionPath.CurrentStep.SourceJobStepName, this.FullyQualifiedJobName,
                    sourceJob.DeferMinutes, numberOfRetries, sourceJob.AutoRetryCount);
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name, msg));
            }
            //retry count exceeded
            else
            {
                string msg = string.Format("Job step: {0} - {1} has exceeded its configured AutoRetryCount, it will NOT be rescheduled - currentretrycount: {2} , autoretrycount:{3})",
                    JED.ExecutionPath.CurrentStep.SourceJobStepName, this.FullyQualifiedJobName,
                    numberOfRetries, sourceJob.AutoRetryCount);
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name, msg));
            }
        }

        protected void ScheduleDynamicJob(JobExecutionDetails newJED)
        {
            newJED.IsRetry = false;
            //newJED.DelayedExecutionMins += 1.0;
            Greenhouse.Data.Model.Core.JobLog jobLog = JobLogger.NewJobLog(newJED);

            Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                string.Format("ScheduleDynamicJob - ContractKey: {0} Step: {1} Calendar: {2} CurrentStep: {3} UserSelections: {4} CustomProperties: {5}",
                newJED.ContractKey, newJED.Step, newJED.ScheduleCalendar,
                (newJED.ExecutionPath.CurrentStep == null ? "null" : newJED.ExecutionPath.CurrentStep.SourceJobStepName),
                newJED.JobProperties.DumpToString(),
                newJED.JobProperties.DumpToString())));
            //TO-DO - fix this
            //jobLog.Id = GlanceDataProvider.SaveJobLog(jobLog);

            Quartz.IJobDetail job = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateJob(newJED, this.Scheduler);
            job.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(jobLog));

            Quartz.ITrigger trigger = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateTrigger(newJED);
            Scheduler.ScheduleJob(job, trigger);
        }

        public void ScheduleBatchChainedJobs(List<JobExecutionDetails> chainedJEDs)
        {
            var chainedJedsBySource = new List<List<JobExecutionDetails>>();
            IEnumerable<string> cachedKeys = null;

            Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                $"{this.JED.JobGUID} - CacheStore Keys:{String.Join(",", CacheStore.GetKeys())}; Chained JobCacheKeys: {String.Join(",", chainedJEDs.Select(x => x.JobCacheKey))}"));
            //0. Get all integration specific keys
            cachedKeys = CacheStore.GetKeys().Where(k => k.StartsWith("JOB_") && !k.EndsWith("ALLINTEGRATIONS"));
            Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                $"{this.JED.JobGUID} - CacheStore Keys (excludes any ending with ALLINTEGRATIONS): {String.Join(",", cachedKeys)}"));

            //1. There are jobs running.
            if (cachedKeys.Any())
            {
                //2. Get sources for which jobs are running.
                var cacheStoreBySource = cachedKeys.Where(k => k.Split('_').Length > 2)
                    .GroupBy(k => k.Split('_')[2]).Select(src => src.Key);
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                    $"{this.JED.JobGUID} - Distinct Sources (CacheStore): {String.Join(",", cacheStoreBySource)}"));
                //3. Sources that have chained JED and waiting to be scheduled.
                chainedJEDs.RemoveAll(x =>
                    cacheStoreBySource.Select(s => s).Contains((x.Source.SourceName)));
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                    $"{this.JED.JobGUID} - Chained JobCacheKeys after removing matches from CacheStore: {String.Join(",", chainedJEDs.Select(x => x.JobCacheKey))}"));
                //There's nothing to schedule; come out of this method.
                if (chainedJEDs.Count == 0)
                {
                    Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                        $"{this.JED.JobGUID} - No new chained jobs to schedule. CacheStore had matching source names for list of chained JEDs."));
                    return;
                }

                //4.Group the chained JED by source for scheduling.
                chainedJedsBySource =
                    chainedJEDs.GroupBy(x => x.Source.SourceID).Select(grp => grp.ToList())
                        .ToList();
            }
            else
            {
                chainedJedsBySource = chainedJEDs.GroupBy(x => x.Source.SourceID).Select(grp => grp.ToList())
                    .ToList();
            }

            foreach (var listJeds in chainedJedsBySource)
            {
                ScheduleChainedJob(listJeds);
                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                    $"{this.JED.JobGUID} - Batch complete - chained - {listJeds.Count} child jobs chained"));
            }
        }

        protected void ScheduleChainedJob(List<JobExecutionDetails> newJEDs)
        {
            IJobDetail childJob = null;

            for (int i = newJEDs.Count - 1; i >= 0; i--)
            {
                var newJED = newJEDs[i];

                newJED.IsRetry = false;
                Greenhouse.Data.Model.Core.JobLog jobLog = JobLogger.NewJobLog(newJED);

                Logger.Log(Msg.Create(LogLevel.Debug, Logger.Name,
                    string.Format("ScheduleChainJob - ContractKey: {0} Step: {1} Calendar: {2} CurrentStep: {3} UserSelections: {4} CustomProperties: {5}",
                        newJED.ContractKey, newJED.Step, newJED.ScheduleCalendar,
                        (newJED.ExecutionPath.CurrentStep == null ? "null" : newJED.ExecutionPath.CurrentStep.SourceJobStepName),
                        newJED.JobProperties.DumpToString(),
                        newJED.JobProperties.DumpToString())));

                Quartz.IJobDetail job = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateJob(newJED, this.Scheduler, isChild: i > 0, childJobKey: childJob?.Key);
                job.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(jobLog));

                childJob = job;

                if (i == 0)
                {
                    Quartz.ITrigger trigger = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateTrigger(newJED);
                    Scheduler.ScheduleJob(job, trigger);
                }
                else
                {
                    Scheduler.AddJob(job, false, true);
                }
            }
        }
    }
    #endregion

}
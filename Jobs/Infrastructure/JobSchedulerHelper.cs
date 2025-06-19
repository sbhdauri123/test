using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Jobs.Infrastructure
{
    public class JobSchedulerHelper : IJobSchedulerHelper
    {
        private readonly NLog.ILogger _logger;
        private readonly IJobLogger _jobLogger;
        private readonly Greenhouse.Caching.ICacheStore _cache;
        private readonly IScheduler _scheduler;

        public JobSchedulerHelper(ILogger logger, IJobLogger jobLogger, ICacheStore cacheStore, IScheduler scheduler)
        {
            _logger = logger;
            _jobLogger = jobLogger;
            _cache = cacheStore;
            _scheduler = scheduler;
        }

        public void ScheduleBatchChainedJobs(JobExecutionDetails JED, List<JobExecutionDetails> chainedJEDs)
        {
            var chainedJedsBySource = new List<List<JobExecutionDetails>>();
            IEnumerable<string> cachedKeys = null;

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                $"{JED.JobGUID} - CacheStore Keys:{String.Join(",", _cache.GetKeys())}; Chained JobCacheKeys: {String.Join(",", chainedJEDs.Select(x => x.JobCacheKey))}"));
            //0. Get all integration specific keys
            cachedKeys = _cache.GetKeys().Where(k => k.StartsWith("JOB_") && !k.EndsWith("ALLINTEGRATIONS"));
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                $"{JED.JobGUID} - CacheStore Keys (excludes any ending with ALLINTEGRATIONS): {String.Join(",", cachedKeys)}"));

            //1. There are jobs running.
            if (cachedKeys.Any())
            {
                //2. Get sources for which jobs are running.
                var cacheStoreBySource = cachedKeys.Where(k => k.Split('_').Length > 2)
                    .GroupBy(k => k.Split('_')[2]).Select(src => src.Key);
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    $"{JED.JobGUID} - Distinct Sources (CacheStore): {String.Join(",", cacheStoreBySource)}"));
                //3. Sources that have chained JED and waiting to be scheduled.
                chainedJEDs.RemoveAll(x =>
                    cacheStoreBySource.Select(s => s).Contains((x.Source.SourceName)));
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    $"{JED.JobGUID} - Chained JobCacheKeys after removing matches from CacheStore: {String.Join(",", chainedJEDs.Select(x => x.JobCacheKey))}"));
                //There's nothing to schedule; come out of this method.
                if (chainedJEDs.Count == 0)
                {
                    _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                        $"{JED.JobGUID} - No new chained jobs to schedule. CacheStore had matching source names for list of chained JEDs."));
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
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    $"{JED.JobGUID} - Batch complete - chained - {listJeds.Count} child jobs chained"));
            }
        }

        private void ScheduleChainedJob(List<JobExecutionDetails> newJEDs)
        {
            IJobDetail childJob = null;

            for (int i = newJEDs.Count - 1; i >= 0; i--)
            {
                var newJED = newJEDs[i];

                newJED.IsRetry = false;
                Greenhouse.Data.Model.Core.JobLog jobLog = _jobLogger.NewJobLog(newJED);

                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    string.Format("ScheduleChainJob - ContractKey: {0} Step: {1} Calendar: {2} CurrentStep: {3} UserSelections: {4} CustomProperties: {5}",
                        newJED.ContractKey, newJED.Step, newJED.ScheduleCalendar,
                        (newJED.ExecutionPath.CurrentStep == null ? "null" : newJED.ExecutionPath.CurrentStep.SourceJobStepName),
                        newJED.JobProperties.DumpToString(),
                        newJED.JobProperties.DumpToString())));

                Quartz.IJobDetail job = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateJob(newJED, _scheduler, isChild: i > 0, childJobKey: childJob?.Key);
                job.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(jobLog));

                childJob = job;

                if (i == 0)
                {
                    Quartz.ITrigger trigger = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateTrigger(newJED);
                    _scheduler.ScheduleJob(job, trigger);
                }
                else
                {
                    _scheduler.AddJob(job, false, true);
                }
            }
        }

        public void ScheduleDynamicJob(JobExecutionDetails newJED)
        {
            newJED.IsRetry = false;
            //newJED.DelayedExecutionMins += 1.0;
            Greenhouse.Data.Model.Core.JobLog jobLog = _jobLogger.NewJobLog(newJED);

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                string.Format("ScheduleDynamicJob - ContractKey: {0} Step: {1} Calendar: {2} CurrentStep: {3} UserSelections: {4} CustomProperties: {5}",
                newJED.ContractKey, newJED.Step, newJED.ScheduleCalendar,
                (newJED.ExecutionPath.CurrentStep == null ? "null" : newJED.ExecutionPath.CurrentStep.SourceJobStepName),
                newJED.JobProperties.DumpToString(),
                newJED.JobProperties.DumpToString())));
            //TO-DO - fix this
            //jobLog.Id = GlanceDataProvider.SaveJobLog(jobLog);

            Quartz.IJobDetail job = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateJob(newJED, _scheduler);
            job.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(jobLog));

            Quartz.ITrigger trigger = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateTrigger(newJED);
            _scheduler.ScheduleJob(job, trigger);
        }

        public void RetryJob(JobExecutionDetails JED, string qualifiedName)
        {
            var sourceJob = JED.ExecutionPath.SourceJobSteps.SingleOrDefault(sj => sj.SourceJobStepID == JED.ExecutionPath.CurrentStep.SourceJobStepID);

            if (sourceJob.AutoRetryCount == 0)
            {
                string msg = string.Format("Job step: {0} - {1} does not have an AutoRetryCount defined, this job will NOT be automatically retried.", JED.ExecutionPath.CurrentStep.SourceJobStepName, qualifiedName);
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, msg));
                return;
            }
            //get in-memory count of how many retries it has already attempted
            string retryCacheKey = string.Format("RETRY_COUNT:{0}", JED.JobGUID);
            int numberOfRetries = _cache.Exists(retryCacheKey) ? _cache.Get<int>(retryCacheKey) : 0;
            //we can retry
            if (numberOfRetries < sourceJob.AutoRetryCount)
            {
                JED.IsRetry = true;
                numberOfRetries++;
                _cache.Set<int>(retryCacheKey, numberOfRetries, new TimeSpan(3650, 0, 0, 0));
                RescheduleExistingJob(JED, sourceJob.DeferMinutes);
                string msg = string.Format("Job step: {0} - {1} has been rescheduled - Defer mins: {2}, currentretrycount < autoretrycount ({3} < {4})",
                    JED.ExecutionPath.CurrentStep.SourceJobStepName, qualifiedName,
                    sourceJob.DeferMinutes, numberOfRetries, sourceJob.AutoRetryCount);
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, msg));
            }
            //retry count exceeded
            else
            {
                string msg = string.Format("Job step: {0} - {1} has exceeded its configured AutoRetryCount, it will NOT be rescheduled - currentretrycount: {2} , autoretrycount:{3})",
                    JED.ExecutionPath.CurrentStep.SourceJobStepName, qualifiedName,
                    numberOfRetries, sourceJob.AutoRetryCount);
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, msg));
            }
        }

        private void RescheduleExistingJob(JobExecutionDetails JED, int additionalMinutes)
        {
            string msg = "Rescheduling existing job: {0} for {1} minutes";
            _logger.Log(Msg.Create(LogLevel.Trace, _logger.Name, string.Format(msg, JED.JobName, additionalMinutes)));

            JED.DelayedExecutionMins = additionalMinutes;

            ITrigger trigger = Infrastructure.JobAndTriggerBuilder.CreateTrigger(JED, true);
            // Quartz v3 uses JSON serialization
            JobLog newJobLog = Utilities.UtilsIO.DeepCloneJson<JobLog>(_jobLogger.JobLog);

            trigger.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(newJobLog));

            IJobDetail job = Infrastructure.JobAndTriggerBuilder.CreateJob(JED, _scheduler, true);
            //make sure job name is completely unique
            string jobName = string.Format("{0}_RS{1}_{2}", job.Key.Name, JED.FailureCount, trigger.StartTimeUtc.Ticks);
            _logger.Log(Msg.Create(LogLevel.Trace, _logger.Name, string.Format("Job: {0} Scheduling New Job with: {1} PreviousFire: {2} NewFire: {3}", job.Key.Name, trigger.Key.Name, DateTime.Now, trigger.StartTimeUtc)));

            _scheduler.ScheduleJob(job, trigger);
        }

    }
}

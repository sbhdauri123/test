using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using NLog;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure
{
    public class ScheduledJob : BaseQuartzJob, IJob
    {
        private readonly NLog.ILogger _logger;

        // Quartz job factory requires an empty constructor
        public ScheduledJob()
        {
        }

        public ScheduledJob(NLog.ILogger logger, IJobLoggerFactory jobLoggerFactory, ISchedulerFactory schedulerFactory,
            Greenhouse.Caching.ICacheStore cache) : base(jobLoggerFactory, schedulerFactory, cache)
        {
            _logger = logger;
        }

        public new Greenhouse.Logging.IJobLogger JobLogger { get; set; }
        public new Greenhouse.Caching.ICacheStore CacheStore { get; set; }
        public new Quartz.IScheduler Scheduler { get; set; }

        public async Task Execute(IJobExecutionContext context)
        {
            this.JobLogger = JobLoggerFactory.GetJobLogger();
            this.Scheduler = await SchedulerFactory.GetScheduler();
            this.CacheStore = Cache;

            JobExecutionDetails jed = Newtonsoft.Json.JsonConvert.DeserializeObject<JobExecutionDetails>(context.MergedJobDataMap.GetString(Constants.JOB_EXECUTION_DETAILS));
            jed.InitializeFromStart();

            Dictionary<string, object> jobDataMap = new Dictionary<string, object>();
            jobDataMap[Constants.JOB_EXECUTION_DETAILS] = jed;

            JobLog jl = null;
            if (context.MergedJobDataMap[Constants.JOB_LOG] != null)
            {
                LogEventInfo lei1 = Msg.Create(LogLevel.Info, _logger.Name, "ScheduledJob MergedJobDataMap not null");
                _logger.Log(lei1);
                jl = Newtonsoft.Json.JsonConvert.DeserializeObject<JobLog>(context.MergedJobDataMap.GetString(Constants.JOB_LOG));
                jl.JobGUID = jed.JobGUID;
                jl.Message = string.Empty;
                jl.Status = Constants.JobLogStatus.Running.ToString();
            }
            else
            {
                LogEventInfo lei2 = Msg.Create(LogLevel.Info, _logger.Name, "ScheduledJob MergedJobDataMap IS NULL");
                _logger.Log(lei2);
            }
            LogEventInfo lei = Msg.Create(LogLevel.Info, _logger.Name, string.Format("ScheduledJob JobLog: {0}", jl));
            _logger.Log(lei);

            jobDataMap[Constants.JOB_LOG] = jl;

            JobLogger.Initialize(jobDataMap);
            JobLogger.Start();

            jed.Step = Constants.JobStep.Initialize;

            Quartz.IJobDetail job = JobAndTriggerBuilder.CreateJob(jed, this.Scheduler);
            // Quartz v3 uses JSON serialization
            JobLog newJL = Greenhouse.Utilities.UtilsIO.DeepCloneJson<JobLog>(JobLogger.JobLog);
            //TODO: Identity?
            //newJL.GUID = Guid.NewGuid();
            job.JobDataMap.Put(Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(newJL));

            //WithMisfireHandlingInstructionNextWithRemainingCount
            Quartz.ITrigger trigger = JobAndTriggerBuilder.CreateTrigger(jed);

            JobLogger.Finish();
            try
            {
                if (await Scheduler.CheckExists(job.Key))
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, string.Format("==>ScheduledJob Warning: JobKey: {0} | Job not created: A Job with the same key already exists.", job.Key.Name)));
                    return;
                }
                else
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, string.Format("==>ScheduledJob About to Schedule: JobKey: {0} TriggerKey: {1} ", job.Key, trigger.Key)));
                    await Scheduler.ScheduleJob(job, trigger);
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, "==>ScheduledJob Scheduled"));
                }
            }
            catch (Exception exc)
            {
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, string.Format("==>ScheduledJob Error: {0} : {1}", exc.Message, exc.StackTrace)));
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, string.Format("==>ScheduledJob INNER: {0} ", exc.InnerException == null ? "NULL" : exc.InnerException.StackTrace)));
            }
        }
    }
}

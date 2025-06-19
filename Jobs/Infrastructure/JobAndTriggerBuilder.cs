using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using NLog;
using Quartz;
using Quartz.Impl.Matchers;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Infrastructure
{
    public static class JobAndTriggerBuilder
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public static IJobDetail CreateJob(JobExecutionDetails jed, Quartz.IScheduler scheduler, bool reschedule = false, bool isChild = false, JobKey childJobKey = null)
        {
            string jobName = reschedule ? string.Format("{0}_RS_{1}", jed.JobName, DateTime.Now.Ticks) : jed.JobName;
            bool isParent = null != childJobKey;

            //a durable job will persist even if there are no triggers associated with it
            //if this job is a child of another job, it needs to be durable as no trigger are attached to it
            bool isDurable = isChild || (jed.Step == Constants.JobStep.Start && jed.ScheduleCalendar.Interval != ScheduleCalendar.IntervalType.Backfill);
            bool requestRecovery = true;
            //adding Aggregate Import Jobs to list of jobs that should not recover (eg Batch jobs)
            //because InitializeAggregateJob should always run first to populate the queue
            //and then schedule the import job immediately after
            if (jed.ContractKey.Contains("Batch") || (jed.Step != Constants.JobStep.Initialize && jed.ExecutionPath.SourceJobSteps.Any(s => s.JobStep.SourceJobStepName == "InitializeAggregateJob")))
            {
                requestRecovery = false;
            }

            System.Diagnostics.StackTrace stackTrace = new System.Diagnostics.StackTrace();
            var meth = stackTrace.GetFrame(1).GetMethod();
            string caller = string.Format("{0}.{1}", meth.DeclaringType, meth.Name);
            string errMsg = "CreateJob - Caller : {0} Type: {1} Name: {2} Group: {3} Description: {4} isDurable: {5} requestRecovery: {6}";

            IJobDetail jobDetail = null;
            if (jed.Step == Constants.JobStep.Start)
            {
                jobDetail = Quartz.JobBuilder.Create<ScheduledJob>()
                          .WithIdentity(jobName, jed.JobGroup)
                          .WithDescription(jed.ExecutionPath.CurrentStep.ShortDescription)
                          .StoreDurably(isDurable)
                          .RequestRecovery(true)
                          .Build();
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format(errMsg,
                    caller, jobDetail.JobType, jobName, jed.JobGroup, jed.ExecutionPath.CurrentStep.ShortDescription, isDurable, "true")));
            }
            else
            {
                jobDetail = Quartz.JobBuilder.Create<ProcessingJob>()
                          .WithIdentity(jobName, jed.JobGroup)
                          .WithDescription(jed.ExecutionPath.CurrentStep.ShortDescription)
                          .StoreDurably(isDurable)
                          .RequestRecovery(requestRecovery)
                          .Build();
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format(errMsg,
                    caller, jobDetail.JobType, jobName, jed.JobGroup, jed.ExecutionPath.CurrentStep.ShortDescription, isDurable, requestRecovery)));
            }

            jobDetail.JobDataMap.Put(Constants.JOB_EXECUTION_DETAILS, jed.ToJSON());

            if (isParent)
            {
                jobDetail.JobDataMap.Put(Constants.JOB_CHILD_JOBKEY, Newtonsoft.Json.JsonConvert.SerializeObject(childJobKey));
            }

            if (isParent || isChild)
            {
                ChainedJobListener chainedJobListener = new ChainedJobListener() { Name = jobDetail.Key.ToString() };
                scheduler.ListenerManager.AddJobListener(chainedJobListener, KeyMatcher<JobKey>.KeyEquals(jobDetail.Key));
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"ChainedJobListener added for {jobDetail.Key}"));
            }

            return jobDetail;
        }

        public static string GetRescheduledTriggerName(JobExecutionDetails jed)
        {
            return string.Format("{0}_RS{1}_{2}", jed.TriggerName, jed.FailureCount, DateTime.Now.Ticks);
        }

        private static ITrigger CreateRescheduleTrigger(JobExecutionDetails jed, bool reschedule = false)
        {
            ITrigger trigger = null;
            double delayMinutes = (jed.DelayedExecutionMins == 0.0 ? 1.0 : jed.DelayedExecutionMins);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateRescheduleTrigger - reschedule: {0} delayMinutes: {1} ", reschedule, delayMinutes)));
            trigger = TriggerBuilder.Create()
                   .WithIdentity(GetRescheduledTriggerName(jed), jed.TriggerGroup)
                   .WithSimpleSchedule()
                   .StartAt(DateTimeOffset.Now.AddMinutes(delayMinutes))
                   .Build();
            return trigger;
        }
        public static ITrigger CreateTrigger(JobExecutionDetails jed, bool reschedule = false)
        {
            if (reschedule)
            {
                return CreateRescheduleTrigger(jed);
            }

            string callerInfo = Utilities.UtilsIO.GetCaller(new System.Diagnostics.StackTrace());
            ITrigger trigger = null;

            switch (jed.Step)
            {

                //if the job is a start job it will be first in the chain so we need to apply the JED StartTime or cronExpression
                case Constants.JobStep.Start:
                    //INTERVAL
                    if (jed.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Minutely ||
                        jed.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Hourly)
                    {
                        int hours = 0;
                        int minutes = 0;
                        if (jed.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Hourly)
                            hours = int.Parse(jed.ScheduleCalendar.IntervalExpression);
                        if (jed.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Minutely)
                            minutes = int.Parse(jed.ScheduleCalendar.IntervalExpression);

                        TimeSpan tsRepeatInterval = new TimeSpan(hours, minutes, 0);
                        //I don't want my start time ovrewritten with UTC now Use the start time from the scheduler
                        if (jed.ExecutionPath.CurrentStep.Step == "Batch")
                        {
                            trigger = TriggerBuilder.Create()
                         .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                         .StartAt(jed.ScheduleCalendar.StartTime)
                         .WithSimpleSchedule(s => s.WithInterval(tsRepeatInterval).RepeatForever().WithMisfireHandlingInstructionNextWithRemainingCount())
                         .Build();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - 1 {0}", callerInfo)));
                        }
                        else
                        {
                            trigger = TriggerBuilder.Create()
                            .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                            .StartAt(jed.ScheduleCalendar.StartTime)
                            .WithSimpleSchedule(s => s.WithInterval(tsRepeatInterval).RepeatForever().WithMisfireHandlingInstructionFireNow())
                            .Build();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - 2 {0}", callerInfo)));
                        }
                        return trigger;
                    }
                    else if (jed.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill)
                    {
                        TimeZoneInfo tzi = System.TimeZoneInfo.FindSystemTimeZoneById(jed.ScheduleCalendar.TimeZoneString);

                        trigger = TriggerBuilder.Create()
                            .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                            .StartAt(jed.ScheduleCalendar.StartTime)
                            .WithSimpleSchedule()
                            .Build();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - Backfill {0}", callerInfo)));
                    }
                    //CRON
                    else
                    {
                        TimeZoneInfo tzi = System.TimeZoneInfo.FindSystemTimeZoneById(jed.ScheduleCalendar.TimeZoneString);

                        if (jed.ContractKey.Contains("Batch"))
                        {
                            trigger = TriggerBuilder.Create()
                            .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                           .StartAt(jed.ScheduleCalendar.StartTime)
                           .WithSchedule(CronScheduleBuilder.CronSchedule(jed.ScheduleCalendar.CronExpression).WithMisfireHandlingInstructionDoNothing().InTimeZone(tzi))
                           .Build();
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - 3 {0}", callerInfo)));
                        }
                        else
                        {
                            trigger = TriggerBuilder.Create()
                            .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                           .StartAt(jed.ScheduleCalendar.StartTime)
                           .WithSchedule(CronScheduleBuilder.CronSchedule(jed.ScheduleCalendar.CronExpression).InTimeZone(tzi))
                           .Build();

                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - 4 {0}", callerInfo)));
                        }
                    }

                    break;

                //otherwise the job is elsewhere in thr ETL chain so it should be an immediate trigger, fire & forget
                default:

                    trigger = TriggerBuilder.Create()
                        .WithIdentity(jed.TriggerName, jed.TriggerGroup)
                        .StartAt(DateBuilder.FutureDate((int)jed.DelayedExecutionMins, IntervalUnit.Minute))
                        .Build();

                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("CreateTrigger - 5 {0}", callerInfo)));

                    break;
            }

            return trigger;
        }

        private sealed class ChainedJobListener : IJobListener
        {
            public string Name { get; set; }

            public static void JobExecutionVetoed(IJobExecutionContext context)
            {
            }

            public Task JobExecutionVetoed(IJobExecutionContext context, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public static void JobToBeExecuted(IJobExecutionContext context)
            {
            }

            public Task JobToBeExecuted(IJobExecutionContext context, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public static async void JobWasExecuted(IJobExecutionContext context, JobExecutionException jobException)
            {
                ArgumentNullException.ThrowIfNull(context);

                var finishedJob = context.JobDetail;

                await context.Scheduler.DeleteJob(finishedJob.Key);
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Job deleted - Key= {finishedJob.Key.ToString()}"));

                var childJobKey = Newtonsoft.Json.JsonConvert.DeserializeObject<JobKey>(context.MergedJobDataMap.GetString(Constants.JOB_CHILD_JOBKEY));
                if (childJobKey == null)
                {
                    return;
                }

                var newJob = await context.Scheduler.GetJobDetail(childJobKey);
                if (newJob == null)
                {
                    return;
                }

                await context.Scheduler.AddJob(newJob, true, false);
                await context.Scheduler.TriggerJob(childJobKey);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Job chained triggered - Key= {newJob.Key.ToString()}"));
            }

            public Task JobWasExecuted(IJobExecutionContext context, JobExecutionException jobException, CancellationToken cancellationToken = default)
            {
                ArgumentNullException.ThrowIfNull(context);

                var finishedJob = context.JobDetail;

                var isSuccess = context.Scheduler.DeleteJob(finishedJob.Key, cancellationToken).Result;

                if (!isSuccess)
                    return Task.CompletedTask;

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Job deleted - Key= {finishedJob.Key.ToString()}"));

                // job is removed from scheduler, so we should also remove its listener
                IJobListener chainedJobListener = context.Scheduler.ListenerManager.GetJobListener(finishedJob.Key.ToString());
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"ChainedJobListener found for {chainedJobListener.Name}"));
                var listenerIsRemoved = context.Scheduler.ListenerManager.RemoveJobListener(chainedJobListener.Name);
                if (listenerIsRemoved)
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"ChainedJobListener removed for job key: {finishedJob.Key}"));

                if (!context.MergedJobDataMap.ContainsKey(Constants.JOB_CHILD_JOBKEY))
                    return Task.CompletedTask;

                var childJobKey = Newtonsoft.Json.JsonConvert.DeserializeObject<JobKey>(context.MergedJobDataMap.GetString(Constants.JOB_CHILD_JOBKEY));
                if (childJobKey == null)
                    return Task.CompletedTask;

                var newJob = context.Scheduler.GetJobDetail(childJobKey, cancellationToken).Result;
                if (newJob == null)
                    return Task.CompletedTask;

                context.Scheduler.AddJob(newJob, true, false, cancellationToken).GetAwaiter().GetResult();
                context.Scheduler.TriggerJob(childJobKey, cancellationToken).GetAwaiter().GetResult();

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Job chained triggered - Key= {newJob.Key.ToString()}"));
                return Task.CompletedTask;
            }
        }
    }
}
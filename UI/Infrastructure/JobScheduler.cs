//using Autofac;
using Greenhouse.Common;
using Greenhouse.Configuration;
using NLog;
using Quartz;
using System.Data.SqlClient;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.UI.Infrastructure
{
    public static class JobScheduler
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        public static Greenhouse.Data.Model.Core.JobExecutionDetails BuildJobExecutionDetail(this Greenhouse.UI.Models.ScheduleJobDetails jobDetails, DateTime beginDate, DateTime endDate, Greenhouse.Data.Model.Core.ScheduleCalendar scheduleCalendar, Greenhouse.Data.Model.Setup.Server server)
        {
            //TODO:RAJEEV
            //This is dummied up until you can adapt it 
            //All jobs should have a Source + SourceJobStep associated with them and defined in the DB
            //Our internal type jobs will have an 'Internal' type of source defined
            //get the source based on user-selection (I'm assuming 'jobDetails' is your model or something similar)
            var source = Data.Services.SetupService.GetById<Data.Model.Setup.Source>(jobDetails.GreenhouseJobScheduler.SourceId);

            var sourceJobStep = Data.Services.SetupService.GetById<Data.Model.Setup.SourceJobStep>(jobDetails.GreenhouseJobScheduler.SourceJobStepID);

            //when user picks a source, it will display a list of 'sets' of job steps on the UI (pulled from the SourceJob JOIN table), query it like this from the UI:
            //var jsn = setupServ.GetSourceJobNames(source.SourceID);

            //I assume this value will come from your model as well
            //var sourceJobName = jobDetails.SourceJobName; //not needed - Scott

            //create JED from the calendar, the source and the sourceJobName, it will initialize everythig else
            var jed = new Greenhouse.Data.Model.Core.JobExecutionDetails(scheduleCalendar, source, server);

            LogEventInfo lei = Greenhouse.Logging.Msg.Create(LogLevel.Info, logger.Name,
                string.Format("==>JobExecutionDetails created in UI Source: {0}, Calendar: {1}", source, scheduleCalendar));
            logger.Log(lei);

            var userSelections = new System.Collections.Specialized.OrderedDictionary();
            if (jobDetails.GreenhouseJobScheduler.SourceId > 0)
            {
                userSelections.Add(Greenhouse.Common.Constants.US_SOURCE_ID, jobDetails.GreenhouseJobScheduler.SourceId);
            }

            if (jobDetails.GreenhouseJobScheduler.AggregateIsBackfill)
            {
                userSelections.Add(Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DATE_FROM, jobDetails.GreenhouseJobScheduler.AggregateBackfillDateFrom);
                userSelections.Add(Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DATE_TO, jobDetails.GreenhouseJobScheduler.AggregateBackfillDateTo);
                userSelections.Add(Greenhouse.Common.Constants.AGGREGATE_BACKFILL_DIM_ONLY, jobDetails.GreenhouseJobScheduler.BackfillDimOnly);
                userSelections.Add(Greenhouse.Common.Constants.AGGREGATE_API_ENTITYIDS, jobDetails.GreenhouseJobScheduler.AggregateAPIEntities);
            }

            jed.JobProperties = userSelections;
            return jed;
        }

        public async static Task<Quartz.IJobDetail> ScheduleJobAsync(this Greenhouse.UI.Models.ScheduleJobDetails scheduleJobDetails
            , SharedJobSchedulers sharedJobSchedulers, bool reschedule = false, Greenhouse.Data.Model.Core.JobLog jobLog = null)
        {
            //WARNING - DO NOT START THIS SCHEDULER
            //Never Point A Non-Clustered Scheduler At the Same Database As Another Scheduler With The Same Scheduler Name
            //If you point more than one scheduler instance at the same set of database tables, and one or more of those instances is not configured for clustering, any of the following may occur:
            //Results in data corruption(deleted data, scrambled data)
            //Results in job seemingly "vanishing" without executing when a trigger's fire time arrives
            //Results in job not executing, "just sitting there" when a trigger's fire time arrives
            //May result in: Dead - locks
            //Other strange problems and data corruption

            IScheduler scheduler = await GetRemotingSchedulerAsync(scheduleJobDetails, sharedJobSchedulers);

            //Get job and trigger
            var job = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateJob(scheduleJobDetails.JED, scheduler, reschedule);

            if (jobLog == null)
            {
                jobLog = new Data.Model.Core.JobLog();
                jobLog.StartDateTime = DateTime.Now;
                jobLog.Status = Greenhouse.Common.Constants.JobLogStatus.Running.ToString();
            }
            jobLog.LastUpdatedDateTime = DateTime.Now;

            job.JobDataMap.Put(Greenhouse.Common.Constants.JOB_LOG, Newtonsoft.Json.JsonConvert.SerializeObject(jobLog));

            try
            {
                var trigger = Greenhouse.Jobs.Infrastructure.JobAndTriggerBuilder.CreateTrigger(scheduleJobDetails.JED, reschedule);
                await scheduler.ScheduleJob(job, trigger);
            }
            catch (System.Net.Sockets.SocketException socketException)
            {
                //The actual exception message contains the IP address & port where the scheduler is listening.
                var logger = NLog.LogManager.GetCurrentClassLogger();
                var lei = Greenhouse.Logging.Msg.Create(LogLevel.Error, logger.Name, String.Format("Original exception: {0}", socketException.Message));
                logger.Log(lei);
                var newException = new JobSchedulerException(String.Format("Could not fetch scheduler. Error id: {0}", lei.Properties[Greenhouse.Logging.Msg.GUID]));
                throw newException;
            }

            return job;
        }

        private async static Task<Quartz.IScheduler> GetRemotingSchedulerAsync(this Greenhouse.UI.Models.ScheduleJobDetails scheduleJobDetails, SharedJobSchedulers sharedJobSchedulers)
        {
            var logger = NLog.LogManager.GetCurrentClassLogger();
            Quartz.IScheduler sched = null;
            Dictionary<string, Quartz.IScheduler> _schedulers = new Dictionary<string, Quartz.IScheduler>();
            try
            {
                string schedulerInstanceName = scheduleJobDetails.ServerAlias;
                string machineName = scheduleJobDetails.ServerName;

                string connectionString = Settings.Current.Quartz.ConnectionString;
                var scsb = new SqlConnectionStringBuilder(connectionString);
                string[] parts = scsb.DataSource.Split(Constants.COMMA_ARRAY);
                string port = parts[1]; //sql server port
                port = string.Format("1{0}", port); //add a 1 as the first number (so 1533 becomes 11533)

                //PS: DO NOT REMOVE THE DEBUG CONSTANTS.
#if LOCALDEV
                machineName = Environment.MachineName;
                schedulerInstanceName = "LOCAL";
#endif
                string tcp = string.Format("tcp://{0}:{1}/QuartzScheduler", machineName, port);

                sched = await sharedJobSchedulers.GetAsync(schedulerInstanceName, tcp, port, scsb);
            }
            catch (System.Net.Sockets.SocketException socketException)
            {
                //The actual exception message contains the IP address & port where the scheduler is listening.
                logger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Error, logger.Name, String.Format("Original exception: {0}", socketException.Message)));
                var newException = new JobSchedulerException("Could not fetch scheduler.");
                throw newException;
            }
            return sched;
        }
    }

    [Serializable]
    internal sealed class JobSchedulerException : Exception
    {
        public JobSchedulerException()
        {
        }

        public JobSchedulerException(string? message) : base(message)
        {
        }

        public JobSchedulerException(string? message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}
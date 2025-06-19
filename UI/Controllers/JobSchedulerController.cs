using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.UI.Infrastructure;
using Greenhouse.UI.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace Greenhouse.UI.Controllers
{
    public class JobSchedulerController : BaseController
    {
        void LogDebug(string message) => _logger.Log(Greenhouse.Logging.Msg.Create(NLog.LogLevel.Debug, _logger.Name, message));

        private NLog.ILogger _logger;

        private Greenhouse.Data.Model.Setup.Server server;

        private SharedJobSchedulers SharedJobSchedulers { get; }

        public JobSchedulerController(NLog.ILogger logger, SharedJobSchedulers sharedJobSchedulers) : base(logger)
        {
            this._logger = logger;
            this.SharedJobSchedulers = sharedJobSchedulers;
        }

        public struct JobTypeUI
        {
            public int ID { get; set; }
            public string Name { get; set; }
            public int? DefaultSourceJobStepId { get; set; }
        }

        public PartialViewResult Index()
        {
            //Get all Job types
            var jobCategories = SetupService.GetAll<JobCategory>();
            var jobTypes = SetupService.GetAll<JobType>();
            var intervals = SetupService.GetAll<Interval>();

            //Get all the values from Scheduler Configuration.
            var schedulerConfig = SetupService.GetAll<SchedulerConfiguration>().OrderBy(x => x.SortOrder).ToList();

            ViewBag.JobTypes = jobTypes.Select(x => new JobTypeUI { ID = x.JobTypeID, Name = x.JobTypeName, DefaultSourceJobStepId = x.DefaultSourceJobStepID });
            ViewBag.JobCategories = jobCategories.Select(x => new Greenhouse.UI.Models.DropdownItem { ID = x.JobCategoryID, Name = x.JobCategoryName });
            ViewBag.Intervals = intervals.Select(x => new Greenhouse.UI.Models.DropdownItem { ID = x.IntervalID, Name = x.IntervalName });
            ViewBag.SchedulerConfiguration = schedulerConfig;

            return PartialView("Index");
        }

        public List<Greenhouse.UI.Models.QuartzTriggerLiteWrapper> GetTriggers(string jobName = null)
        {
            try
            {
                string jobClassName = typeof(Greenhouse.Jobs.Infrastructure.ScheduledJob).FullName;
                var triggers = SetupService.GetQuartzTriggers<Data.Model.Core.QuartzTrigger>().Where(t => t.JOB_CLASS_NAME.Contains(jobClassName)).ToList();

                LogDebug("GetTriggers JobClassName=" + jobClassName);

                if (server == null)
                {
                    //TODO: Find a better way to do this. Register with Autofac?
                    string serverName = System.Environment.MachineName;

#if LOCALDEV
                    serverName = "DTIONEIM01-UE1D";

                    //QA - Machine.Config must be edited as well
                    //serverName = "DTIONEIM01-UE1T";
#endif

                    server = SetupService.GetAll<Server>().FirstOrDefault(x => x.ServerName == serverName);
                }

                LogDebug($"GetTriggers server: ServerName={server?.ServerName} ServerMachineName={server?.ServerMachineName} ServerIP={server?.ServerIP}");

                if (server != null)
                {
                    var tzi = TimeZoneInfo.FromSerializedString(server.TimeZoneString);
                    var liteTriggers = triggers.ConvertAll(t => new Greenhouse.UI.Models.QuartzTriggerLiteWrapper
                    {
                        JobName = t.JOB_NAME,
                        Description = t.DESCRIPTION,
                        JobGroup = t.JOB_GROUP,
                        TriggerGroup = t.TRIGGER_GROUP,
                        TriggerName = t.TRIGGER_NAME,
                        TriggerState = t.TRIGGER_STATE,
                        NextFireTime = t.NEXT_FIRE_TIME != null ? TimeZoneInfo.ConvertTimeFromUtc(new DateTime(t.NEXT_FIRE_TIME.Value), tzi) : DateTime.MinValue,
                        PrevFireTime = t.PREV_FIRE_TIME != null ? TimeZoneInfo.ConvertTimeFromUtc(new DateTime(t.PREV_FIRE_TIME.Value), tzi) : DateTime.MinValue,
                        SchedulerName = t.SCHED_NAME,
                        Interval = GetInterval(t.TRIGGER_TYPE, t.CRON_EXPRESSION, t.REPEAT_INTERVAL, t.JOB_NAME),
                        Selected = false
                    });
                    //.Where(lt => lt.SchedulerName.Equals("ScheduledJob")).ToList()

                    if (jobName != null)
                    {
                        //string jobName = key.ToString().Replace("Stallion.", "");
                        return liteTriggers.Where(x => x.JobName == jobName).ToList();
                    }
                    else return liteTriggers;
                }
                else return null;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
                var errorMessage = String.Format("Error: {0} <br /> <br /> GUID: {1}", ex.Message, base.LogException(ex));

                throw (new JobSchedulerException(errorMessage));
            }
        }

        public string GetInterval(string Trigger_Type, string CRON_EXPRESSION, Int32 REPEAT_INTERVAL, string JOB_NAME)
        {
            string interval = "";
            if (JOB_NAME.EndsWith("~BF"))
            {
                return "Backfill";
            }

            if (Trigger_Type == "SIMPLE")
            {
                decimal Minutes = REPEAT_INTERVAL / 1000 / 60;
                if (Minutes < 60)
                    interval = "Minutely";
                else
                    interval = "Hourly";
            }
            else if (Trigger_Type == "CRON")
            {
                if (CRON_EXPRESSION.EndsWith("MON-SUN"))
                    interval = "Daily";
                else if (CRON_EXPRESSION.EndsWith("L * ?"))
                    interval = "Monthly";
                else
                {
                    string[] x = CRON_EXPRESSION.Split('*');
                    interval = "Weekly (" + x[1].TrimStart() + ")";
                }
            }

            return interval;
        }

        [HttpGet]
        public ActionResult GetJobTypes(int jobTypeId)
        {
            var sourcejobstepTypes = SetupService.sourcejobstepTypes(jobTypeId);

            return Json(sourcejobstepTypes.ToList());
        }

        [HttpGet]
        public ActionResult GetSourceJobStep(int sourceJobStepId)
        {
            var sourcejobstep = SetupService.GetById<SourceJobStep>(sourceJobStepId);

            return Json(sourcejobstep);
        }

        [HttpGet]
        public ActionResult GetAPIEntityIds(int sourceId)
        {
            var results = JobService.GetAllActiveAPIEntities(sourceId);
            return Json(results);
        }

        [HttpGet]
        public ActionResult GetSources(int SourceJobStepID)
        {
            var results = SetupService.GetSources(SourceJobStepID);

            return Json(results.ToList());
        }

        [HttpGet]
        public ActionResult GetScheduledJobs()
        {
            List<QuartzTriggerLiteWrapper> liteTriggers = GetTriggers();
            if (liteTriggers == null) return Json(string.Empty);
            else return Json(liteTriggers);
        }

        [HttpPost]
        public ActionResult DeleteJob(string jobName, string jobGroup)
        {
            try
            {
                var result = SetupService.DeleteJob(jobName, jobGroup);

                var responseMsg = new AjaxResponseMessage
                {
                    Id = jobName + "|" + jobGroup,
                    Message = "Job successfully deleted.",
                    Title = "Delete Trigger Request",
                    Success = true
                };

                var auditLog = new Data.Model.Setup.AuditLog
                {
                    AppComponent = "JobScheduler",
                    Action = Constants.AuditLogAction.Delete.ToString(),
                    AdditionalDetails = JsonConvert.SerializeObject(new
                    {
                        ModifiedValue = responseMsg
                    }),
                    CreatedDate = DateTime.Now
                };

                SaveAuditLog(auditLog);

                return Json(responseMsg);
            }
            catch (Exception ex)
            {
                base.LogException(ex);

                var responseMsg = new AjaxResponseMessage
                {
                    Id = jobName + "|" + jobGroup,
                    Message = "Error deleting trigger: " + ex.Message,
                    Title = "Delete Trigger Request",
                    Success = false
                };
                return Content("Error deleting trigger: " + ex.Message);
            }
        }

        [HttpPost]
        public async Task<ActionResult> ScheduleJob(Greenhouse.UI.Models.GreenhouseJobScheduler model)
        {
            try
            {
                bool internalJob = false;

                var Source = SetupService.GetById<Source>(model.SourceId);
                var sourceJob = SetupService.GetSourceJobs(model.SourceId, model.SourceJobStepID).FirstOrDefault();

                // Internal Jobs - NOT PROCESSING JOBS - will be assigned a SourceID in the db.
                if (Source == null && sourceJob.ExecutionTypeID != 2)
                {
                    internalJob = true;
                    Source = SetupService.GetById<Source>(sourceJob.SourceID);
                    model.SourceId = Source.SourceID;
                }

                var SourceJobStep = SetupService.GetById<SourceJobStep>(model.SourceJobStepID);
                var jobServer = SetupService.GetServers(sourceJob.ExecutionTypeID).FirstOrDefault();

                var jobType = SetupService.GetById<JobType>(model.JobTypeId);

                if (String.IsNullOrEmpty(jobType.FQClassName))
                {
                    jobType.FQClassName = "Greenhouse.Jobs.Infrastructure.ScheduledJob";
                }

                string JobExportName = String.Empty;
                if (SourceJobStep.JobCategoryID == 1)
                    JobExportName = "Generic";
                else
                {
                    if (internalJob)
                        JobExportName = "All";
                    else
                        JobExportName = Source.SourceName;
                }
                JobExportName = JobExportName + SourceJobStep.SourceJobStepName;

                string TimeZoneString = jobServer.TimeZoneString.Substring(0, jobServer.TimeZoneString.IndexOf(Constants.SEMICOLON, 0));

                var scheduleJobDetails = new Greenhouse.UI.Models.ScheduleJobDetails
                {
                    GreenhouseJobScheduler = model,
                    JobExportName = JobExportName,
                    JobServerIP = jobServer.ServerIP,
                    JobType = jobType.FQClassName,
                    ServerName = jobServer.ServerName,
                    ServerAlias = jobServer.ServerAlias,
                    SourceName = Source != null ? Source.SourceName : "Greenhouse", //The Job Framework needs cleaned up to not need this for GH.
                    TimeZoneString = TimeZoneString,
                    SourceID = model.SourceId
                };

                LogDebug($"ScheduleJob scheduleJobDetails: JobExportName = {scheduleJobDetails?.JobExportName} JobServerIP = {jobServer?.ServerIP} JobServerName = {jobServer?.ServerName} JobType = {jobType.FQClassName} ServerName = {jobServer?.ServerName} ServerAlias = {jobServer?.ServerAlias} SourceName = {scheduleJobDetails?.SourceName} TimeZoneString = {TimeZoneString} SourceID = {scheduleJobDetails?.SourceID} scheduleJobDetails.GreenhouseJobScheduler.Interval={scheduleJobDetails?.GreenhouseJobScheduler?.Interval}");

                Greenhouse.Data.Model.Core.ScheduleCalendar cal = null;
                DateTime beginDate = new DateTime(2000, 01, 01);
                DateTime endDate = new DateTime(2000, 01, 01);
                DateTime dateTimeForJob = DateTime.Now;
                if (!String.IsNullOrEmpty(scheduleJobDetails.GreenhouseJobScheduler.Time))
                {
                    dateTimeForJob = Convert.ToDateTime(scheduleJobDetails.GreenhouseJobScheduler.Time);
                    beginDate = endDate = dateTimeForJob;
                }

                switch (scheduleJobDetails.GreenhouseJobScheduler.Interval)
                {
                    case "Daily":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Daily);
                        break;
                    case "Weekly":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Weekly, scheduleJobDetails.GreenhouseJobScheduler.Days);
                        break;
                    case "Monthly":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Monthly, "L");
                        break;
                    /*Minutely & Hourly jobs are setup as SimpleSchedule jobs that doesn't take TimeZoneInfo input.
                     Other jobs are built as CronSchedule that takes TimeZoneInfo as input.*/
                    case "Minutely":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Minutely, scheduleJobDetails.GreenhouseJobScheduler.Minutes);
                        dateTimeForJob = TimeZoneInfo.ConvertTimeToUtc(dateTimeForJob, TimeZoneInfo.FindSystemTimeZoneById(TimeZoneString));
                        break;
                    case "Hourly":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Hourly, scheduleJobDetails.GreenhouseJobScheduler.Hours);
                        //dateTimeForJob = TimeZoneInfo.ConvertTimeToUtc(dateTimeForJob, TimeZoneInfo.FindSystemTimeZoneById(jobServer.TimeZoneString));
                        dateTimeForJob = TimeZoneInfo.ConvertTimeToUtc(dateTimeForJob, TimeZoneInfo.FromSerializedString(jobServer.TimeZoneString));
                        break;
                    case "Backfill":
                        cal = new Greenhouse.Data.Model.Core.ScheduleCalendar(TimeZoneString, Greenhouse.Data.Model.Core.ScheduleCalendar.IntervalType.Backfill, scheduleJobDetails.GreenhouseJobScheduler.Time);
                        dateTimeForJob = TimeZoneInfo.ConvertTimeToUtc(dateTimeForJob, TimeZoneInfo.FromSerializedString(jobServer.TimeZoneString));
                        break;
                }

                cal.StartTime = dateTimeForJob;
                LogDebug($"ScheduleJob beginDate={beginDate} endDate={endDate}");

                //Get the job execution details
                var jed = scheduleJobDetails.BuildJobExecutionDetail(beginDate, endDate, cal, jobServer);
                LogDebug($"ScheduleJob JED: ContractKey={jed?.ContractKey} JobName={jed?.JobName} JobCacheKey={jed?.JobCacheKey} JobGroup={jed?.JobGroup} JobGroup={jed?.JobGroup} " +
                         $"DataLoadJobName={jed?.DataLoadJobName} JobServer={jed?.JobServer} JobGUID={jed?.JobGUID}");

                jed.JobProperties[Greenhouse.Common.Constants.CP_TIMEZONE_STRING] = scheduleJobDetails.TimeZoneString;

                //jed.SubType = schedulerConfig.DisplayName;

                scheduleJobDetails.JED = jed;

                Quartz.IJobDetail job = await scheduleJobDetails.ScheduleJobAsync(this.SharedJobSchedulers);

                LogDebug($"ScheduleJob JOB: Key={job.Key} Type={job.JobType}");

                var data = GetTriggers(scheduleJobDetails.JED.JobName);

                var auditLog = new Data.Model.Setup.AuditLog
                {
                    AppComponent = "JobScheduler",
                    Action = Constants.AuditLogAction.Create.ToString(),
                    AdditionalDetails = JsonConvert.SerializeObject(new
                    {
                        ModifiedValue = model
                    }),
                    CreatedDate = DateTime.Now
                };

                SaveAuditLog(auditLog);

                if (data == null)
                    return Json(new { });
                else
                    return new JsonResult(new { Data = data });
            }
            catch (Exception ex)
            {
                var errorMessage = String.Format("Error: {0} <br /> <br /> GUID: {1}", ex.Message, base.LogException(ex));
                return Content(errorMessage);
            }
        }

        [HttpPost]
        public ActionResult CheckJobAlreadyExists(string Description, string JobGroup)
        {
            var result = SetupService.CheckJobExists<Data.Model.Core.QuartzTrigger>(Description, JobGroup);

            if (result.Any()) return Json(true);
            else return Json(false);
        }

        [HttpPost]
        public ActionResult UpdateJobNextFireTime(Greenhouse.UI.Models.GreenhouseJobScheduler model)
        {
            try
            {
                string serverName = System.Environment.MachineName;

#if LOCALDEV
                serverName = "DTIONEIM01-UE1D"; 

                //QA
                //serverName = "DTIONEIM01-UE1T";
#endif

                server = SetupService.GetAll<Server>().FirstOrDefault(x => x.ServerName == serverName);
                var tzi = TimeZoneInfo.FromSerializedString(server.TimeZoneString);

                var nextfiretimeForJob = Convert.ToDateTime(model.Time);

                var result = SetupService.UpdateJobNextFireTime(model.TriggerName, TimeZoneInfo.ConvertTimeToUtc(new DateTime(nextfiretimeForJob.Ticks), tzi).Ticks);

                var responseMsg = new AjaxResponseMessage
                {
                    Message = "Job successfully updated.",
                    Title = "Update Job Request",
                    Success = true
                };

                var auditLog = new Data.Model.Setup.AuditLog
                {
                    AppComponent = "JobScheduler",
                    Action = Constants.AuditLogAction.Update.ToString(),
                    AdditionalDetails = JsonConvert.SerializeObject(new
                    {
                        Summary = $"TriggerName: {model.TriggerName}; Next Fire Time: {nextfiretimeForJob}",
                        ModifiedValue = responseMsg
                    }),
                    CreatedDate = DateTime.Now
                };

                SaveAuditLog(auditLog);

                return Json(responseMsg);
            }
            catch (Exception ex)
            {
                base.LogException(ex);

                var responseMsg = new AjaxResponseMessage
                {
                    Message = "Error updating NextFireTime: " + ex.Message,
                    Title = "Update Job Request",
                    Success = false
                };
                return Content("Error updating NextFireTime: " + ex.Message);
            }
        }
    }
}
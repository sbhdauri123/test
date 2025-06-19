using Greenhouse.Common;
using Greenhouse.Configuration;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Logging;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.UI.Controllers
{
    public class JobLogController : BaseController
    {
        private readonly SourceRepository sourceRepository = new SourceRepository();

        public JobLogController(NLog.ILogger logger) : base(logger)
        {

        }

        // GET: JobLog
        public ActionResult Index()
        {
            return View();
        }

        public class SourceInfo
        {
            public int SourceID { get; set; }
            public string SourceName { get; set; }
            public int IntegrationCount { get; set; }
            public bool HasIntegrationJobsChained { get; set; }
            public AggregateProcessingSettings AggregateProcessingSettings { get; set; }

        }
        [HttpGet]
        public JsonResult GetAllJobLogs()
        {
            IEnumerable<dynamic> logs = Data.Services.JobService.GetAllJobLogs();
            var jobStatusList = GetStatusList();

            var aggregateSources = GetAggregateSources();

            var env = Settings.Current.Application.Environment.ToLower();
            var splunkIndex = (env.Equals("prod", StringComparison.CurrentCultureIgnoreCase)) ? $"datalake" : $"{env}_datalake";

            var searchSplunkValue = Data.Services.SetupService.GetById<Data.Model.Setup.Lookup>(Common.Constants.SPLUNK_SEARCH).Value;

            var jobLogsLite = from l in logs
                              join s in jobStatusList on l.Status equals s.Status
                              let statusText = s.Status
                              //let jobLogDetails = Newtonsoft.Json.JsonConvert.DeserializeObject<Greenhouse.Data.Model.Core.JobLogDetails>(l.JobLogDetailsJSON)
                              select new Models.JobLogLite
                              {
                                  JobLogID = l.JobLogID,
                                  IntegrationName = GetIntegrationName(l, aggregateSources),
                                  SourceName = l.SourceName,
                                  StepDescription = l.StepDescription,
                                  JobStatus = l.Status,
                                  LastUpdated = l.LastUpdatedDateTime,
                                  StartDateTime = l.StartDateTime,
                                  Message = l.Message,
                                  StatusSortOrder = s.SortOrder,
                                  JobGUID = Convert.ToString(l.JobGUID),
                                  SearchSplunk = System.Net.WebUtility.UrlEncode(string.Format(searchSplunkValue, splunkIndex, Convert.ToString(l.JobGUID))),
                                  FileLogCount = l.FileLogCount,
                                  JobDescription = l.JobDescription,
                                  LogCount = l.LogCount
                              };

            var orderedJobLogs = jobLogsLite.OrderBy(x => x.StatusSortOrder).ThenByDescending(y => y.JobLogID).ThenByDescending(y => y.LastUpdated).ToList();

            return new JsonResult(orderedJobLogs);
        }

        private List<SourceInfo> GetAggregateSources()
        {
            var allSources = sourceRepository.GetAllSourceInfo();

            var aggregateSources = allSources
                .Where(s => s.ETLTypeID == Constants.UI_ETLTYPEID_REDSHIFT && s.IntegrationIsActive)
                .GroupBy(s => s.SourceID)
                .Select(s => new SourceInfo
                {
                    SourceID = s.Key,
                    SourceName = s.First().SourceName,
                    HasIntegrationJobsChained = s.First().HasIntegrationJobsChained,
                    IntegrationCount = s.Count(),
                    AggregateProcessingSettings = s.First().AggregateProcessingSettings
                });
            return aggregateSources.ToList();
        }

        private static string GetIntegrationName(dynamic l, List<SourceInfo> aggregateSources)
        {
            // aggregate sources are processed at the source level, so Integration name :
            // if source has chained integrations: show IntegrationName
            // else if 1 active Integration for that source: show IntegrationName
            // else if multiple active Integrations: show 'All'
            // else if source AggregateProcessingSettings.IntegrationProcessingRequired is true: show IntegrationName

            string step = (string)l.StepDescription;
            if (!step.Equals(Constants.ExecutionType.Processing.ToString(), StringComparison.InvariantCultureIgnoreCase)) return l.IntegrationName;

            var currentSource = aggregateSources.Find(s => s.SourceID == l.SourceID);
            bool isAggregateSource = currentSource != null;
            if (isAggregateSource && !currentSource.HasIntegrationJobsChained && !currentSource.AggregateProcessingSettings.IntegrationProcessingRequired
                                  && currentSource.IntegrationCount > 1)
            {
                return "All";
            }

            return l.IntegrationName;
        }

        [HttpPost]
        public ActionResult DeleteLog(Models.JobLogLite model)
        {
            Int64 id = model.JobLogID;

            try
            {
                var auditLog = new Data.Model.Setup.AuditLog
                {
                    AppComponent = "JobLog",
                    Action = Constants.AuditLogAction.Delete.ToString(),
                    AdditionalDetails = JsonConvert.SerializeObject(new
                    {
                        ModifiedValue = new
                        {
                            model.JobLogID,
                            model.StepDescription,
                            model.JobDescription,
                            model.JobStatus,
                            model.SourceName,
                            model.IntegrationName,
                            Message = "", //we don't need the error message displayed in audit log
                            model.LastUpdated,
                            model.StartDateTime,
                            model.JobGUID,
                            model.LogCount,
                            model.ExecutionTime,
                            model.StatusSortOrder,
                            model.FileLogCount,
                            model.SearchSplunk,
                            model.SplunkIndex
                        }

                    }),
                    CreatedDate = DateTime.Now
                };

                var repo = new Data.Repositories.JobLogRepository();
                var count = repo.DeleteJobLogs(id, model.JobStatus);

                SaveAuditLog(auditLog);

                this.JobLogger.Log(Msg.Create(LogLevel.Info, "UI.JobLog", string.Format("Successfully delete JobLogID {0} with count {1}; {2}", id, count, model.JobGUID)));
                if (model.JobStatus.Equals("error", StringComparison.CurrentCultureIgnoreCase))
                {
                    if (count > 1)//only related job logs were deleted. Latest job log remaining
                    {
                        model.LogCount = 1;
                    }
                    else
                    {
                        model.LogCount = 0;
                    }
                }
                else
                {
                    model.LogCount = 0;
                }
            }
            catch (Exception exc)
            {
                this.JobLogger.Log(Msg.Create(LogLevel.Error, "UI.JobLog", string.Format("Error trying to delete JobLogID {0};", id), exc));
            }

            return new JsonResult(model);
        }

        // POST: JobLog/Delete/5
        [HttpPost]
        public bool RetryJob(string jobLogGUID)
        {
            bool retryJobStatus = false;
            try
            {
                var jobGUID = System.Guid.Parse(jobLogGUID);
                // TODO: Add delete logic here
                retryJobStatus = true;
            }
            catch (Exception exc)
            {
                this.JobLogger.Log(Msg.Create(LogLevel.Error, "UI.JobLog", string.Format("Error retrying job guid {0};", jobLogGUID), exc));
            }

            return retryJobStatus;
        }

        [HttpGet]
        public JsonResult GetFileLogsPerJob(Int64 id)
        {
            var parameters = new KeyValuePair<string, string>[]
             {
                 new KeyValuePair<string, string>("JobLogID", id.ToString())
             };
            var queues = Data.Services.SetupService.GetAll<Models.JobQueueModel>("GetFileLogsPerJob", parameters);
            return new JsonResult(queues);
        }

        private static List<JobLogStatus> GetStatusList()
        {
            //Cannot use the JobStatus enum defined in Constants.cs because the order of sorting does not map to enum values.
            var statusList = new List<JobLogStatus>();
            statusList.Add(new JobLogStatus { Status = "Error", SortOrder = 1 });
            statusList.Add(new JobLogStatus { Status = "Warning", SortOrder = 2 });
            statusList.Add(new JobLogStatus { Status = "Running", SortOrder = 3 });
            statusList.Add(new JobLogStatus { Status = "Pending", SortOrder = 4 });
            statusList.Add(new JobLogStatus { Status = "Throttled", SortOrder = 5 });
            statusList.Add(new JobLogStatus { Status = "Complete", SortOrder = 6 });

            return statusList;
        }
    }

    public class JobLogStatus
    {
        public string Status { get; set; }
        public int SortOrder { get; set; }
    }
}

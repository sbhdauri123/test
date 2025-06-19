using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Newtonsoft.Json;
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Text;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class JobExecutionDetails
    {
        public const char FIELD_DELIMITER = '~';

        public Guid JobGUID { get; set; }

        //#region Constructors

        public JobExecutionDetails(ScheduleCalendar cal, Source source, Server server)
        {
            JobGUID = Guid.NewGuid();
            this.ScheduleCalendar = cal;
            this.Source = source;
            this.JobServer = server;
            this.Step = Constants.JobStep.Start;
            InitializeFromStart();
        }

        //only to be called when the ScheduledJob (the one on the CRON trigger in Quartz) kicks off, this
        //will ensure that it always pulls the latest-greatest steps from the DB
        public void InitializeFromStart()
        {
            int? sourceId = this.Source == null ? (int?)null : this.Source.SourceID;
            var epSteps = SetupService.GetMappedSourceJobs(sourceId, this.JobServer.ExecutionTypeID);
            if (!epSteps.Any())
            {
                throw new InvalidOperationException(string.Format("Unable to initialize ExecutionPath for SourceId:{0} and ExecutionTypeId: {1}. There are no entries configured to match these values. Verify that SourceJobSteps are configured correctly in the database.", sourceId, this.JobServer.ExecutionTypeID));
            }
            ExecutionPath = new Core.ExecutionPath(epSteps);
        }

        #region Properties

        public ScheduleCalendar ScheduleCalendar { get; set; }

        public ExecutionPath ExecutionPath { get; set; }

        public Source Source { get; set; }
        private string GetSourceName()
        {
            if (Source == null)
            {
                return "All";
            }
            else
            {
                return Source.SourceName;
            }
        }

        public Server JobServer { get; set; }

        private OrderedDictionary _jobProperties = new OrderedDictionary();
        public OrderedDictionary JobProperties
        {
            get
            {
                return _jobProperties;
            }
            set
            {
                _jobProperties = value;
            }
        }

        private Constants.JobStep _step;
        public Constants.JobStep Step
        {
            get { return _step; }
            set
            {
                if (value != _step)
                {
                    //if the step changes we always want to zero out the DelayedExecution value so it doesn't effect future triggers
                    DelayedExecutionMins = 0;
                }
                _step = value;
            }
        }
        public int FailureCount { get; set; }

        //Note: DelayedExecutionMins are reset to zero each time the Step changes so set this property after setting the step if you want it to be non-zero
        private double _delayedExecutionMins;
        public double DelayedExecutionMins
        {
            get
            {
                return _delayedExecutionMins;
            }
            set
            {
                _delayedExecutionMins = value;
                //TODO: Scott to verify.
                if (ScheduleCalendar != null)
                {
                    ScheduleCalendar.StartTime.AddMinutes(_delayedExecutionMins);
                }
            }
        }

        [JsonIgnore]
        public string ContractKey
        {
            get
            {
                //default to Source
                string prefix = GetSourceName(); //if Source is null then it uses "All", otherwise it uses Source.SourceName
                //but if Source is not null and it's a Generic job, use "Generic" as prefix
                if (Source != null && this.ExecutionPath.CurrentStep.JobCategoryID == (int)Constants.SourceJobCategory.Generic)
                {
                    prefix = "Generic";
                }
                return string.Format("{0}{1}", prefix, this.ExecutionPath.CurrentStep.SourceJobStepName);
            }
        }

        public bool IsRetry { get; set; }

        #endregion

        public string ToJSON()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings() { MissingMemberHandling = MissingMemberHandling.Ignore, NullValueHandling = NullValueHandling.Ignore });
        }

        #region Naming

        [JsonIgnore]
        public string JobCacheKey
        {
            get
            {
                string src = Source == null ? "ALLSOURCES" : Source.SourceName;
                string integ = this.JobProperties[Constants.US_INTEGRATION_ID] == null ? "ALLINTEGRATIONS" : this.JobProperties[Constants.US_INTEGRATION_ID].ToString();

                // adding suffix allows "daily" and "backfill" initialize-aggregate jobs to run simultaneously
                // in Greenhouse.Jobs.Infrastructure.ProcessJob there is a check that prevents the same job cache key from running
                string backfillSuffix = this.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill ? $"_BF" : string.Empty;

                return string.Format("JOB_{0}_{1}_{2}{3}", ExecutionPath.CurrentStep.SourceJobStepName, src, integ, backfillSuffix);
            }
        }

        /// <summary>
        /// Get the scheduler job name to use for a standard stateless processing job
        /// </summary>
        /// <returns>the jobname</returns>
        [JsonIgnore]
        public string ProcessingJobName
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(GetSourceName()).Append(FIELD_DELIMITER).Append(this.Step).Append(FIELD_DELIMITER);
                sb.Append(this.ExecutionPath.CurrentStep.SourceJobStepName);
                sb.Append(string.IsNullOrEmpty(this.ExecutionPath.CurrentStep.SubType) ? string.Empty : string.Format("-{0}", this.ExecutionPath.CurrentStep.SubType));
                sb.Append(JobGUID);

                if (this.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill)
                {
                    sb.Append(FIELD_DELIMITER).Append("BF");
                }

                return sb.ToString();
            }
        }

        /// <summary>
        /// Get the scheduler job name to use for a stateful data-load job.
        /// For stateful jobs we want to combination of JOB_NAME + JOB_GROUP to be unique
        /// JOB_GROUP will always be the DataSource name while the JOB_NAME will be ClientName + "_DataLoad"
        /// </summary>
        /// <returns>the jobname</returns>
        [JsonIgnore]
        public string DataLoadJobName
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(GetSourceName()).Append(FIELD_DELIMITER).Append(this.Step).Append(FIELD_DELIMITER);
                sb.Append(this.ExecutionPath.CurrentStep.SourceJobStepName);
                sb.Append(string.IsNullOrEmpty(this.ExecutionPath.CurrentStep.SubType) ? string.Empty : string.Format("-{0}", this.ExecutionPath.CurrentStep.SubType));
                //sb.Append(FIELD_DELIMITER).Append(_clientName);
                sb.Append(FIELD_DELIMITER).Append(JobGUID);

                if (this.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill)
                {
                    sb.Append(FIELD_DELIMITER).Append("BF");
                }

                return sb.ToString();
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns>the jobname</returns>
        [JsonIgnore]
        public string JobName
        {
            get
            {
                return (this.Step == Constants.JobStep.DataLoad ? DataLoadJobName : ProcessingJobName);
            }
        }

        /// <summary>
        /// Get the group for the job.
        /// For now it is simply the name of the datasource
        /// </summary>
        /// <returns>the job group</returns>
        public string JobGroup
        {
            get
            {
                return GetSourceName();
            }
        }

        /// <summary>
        /// Get the group for the trigger.
        /// For now it is simply the name of the datasource
        /// </summary>
        /// <returns>the triggergroup</returns>
        public string TriggerGroup
        {
            get
            {
                if (ScheduleCalendar is null)
                {
                    return string.Empty;
                }

                StringBuilder sb = new StringBuilder();
                sb.Append((this.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill) ? "TG_BACKFILL" : "TG_STANDARD");
                sb.Append(':').Append(GetSourceName());
                return sb.ToString();
            }
        }

        /// <summary>
        /// Get a unique name for the trigger.
        /// </summary>
        /// <returns>the triggername</returns>
        public string TriggerName
        {
            get
            {
                if (ScheduleCalendar is null)
                {
                    return string.Empty;
                }

                StringBuilder sb = new StringBuilder();
                sb.Append(GetSourceName()).Append(FIELD_DELIMITER).Append(this.Step).Append(FIELD_DELIMITER);
                sb.Append(this.ExecutionPath.CurrentStep.SourceJobStepName);
                sb.Append(string.IsNullOrEmpty(this.ExecutionPath.CurrentStep.SubType) ? string.Empty : string.Format("-{0}", this.ExecutionPath.CurrentStep.SubType));
                sb.Append(this.Step.ToString()).Append(FIELD_DELIMITER);
                sb.Append(this.JobGUID);

                if (this.ScheduleCalendar.Interval == ScheduleCalendar.IntervalType.Backfill)
                {
                    sb.Append(FIELD_DELIMITER).Append("BF");
                }

                return sb.ToString();
            }
        }

        [JsonConstructor]
        private JobExecutionDetails()
        {
        }

        #endregion

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            Type t = this.GetType();
            System.Reflection.PropertyInfo[] pis = t.GetProperties();
            for (int i = 0; i < pis.Length; i++)
            {
                System.Reflection.PropertyInfo pi = (System.Reflection.PropertyInfo)pis.GetValue(i);
                sb.AppendFormat("{0}: {1}, ", pi.Name, pi.GetValue(this, Array.Empty<object>()));
            }
            return sb.ToString();
        }

        public void ResetExecutionGuid()
        {
            JobGUID = Guid.NewGuid();
        }
    }
}

namespace Greenhouse.UI.Models
{
    [Serializable]
    public partial class GreenhouseJobScheduler
    {
        public GreenhouseJobScheduler()
        {
        }

        public int SourceId { get; set; }
        public int JobTypeId { get; set; }
        public string Interval { get; set; }
        public string Time { get; set; }
        public string Days { get; set; }
        public string Minutes { get; set; }
        public string Hours { get; set; }
        public string TriggerName { get; set; }
        public int SourceJobStepID { get; set; }
        public bool AggregateIsBackfill { get; set; }
        public string AggregateBackfillDateFrom { get; set; }
        public string AggregateBackfillDateTo { get; set; }
        public List<string> AggregateAPIEntities { get; set; }
        public bool BackfillDimOnly { get; set; }

        #region Created/Updated Dates
        public DateTime CreatedDate
        {
            get
            {
                return (this.createdDate == default(DateTime))
                   ? this.createdDate = DateTime.Now
                   : this.createdDate;
            }

            set { this.createdDate = value; }
        }
        private DateTime createdDate;
        public DateTime LastUpdated
        {
            get
            {
                return (this.lastUpdated == default(DateTime))
                   ? this.lastUpdated = DateTime.Now
                   : this.lastUpdated;
            }

            set { this.lastUpdated = value; }
        }
        private DateTime lastUpdated;
        #endregion

    }

    [Serializable]
    public class SourceJobStepTypes
    {
        public int SourceJobStepID { get; set; }
        public string ShortDescription { get; set; }
        public bool IsBatch { get; set; }
        public int? SourceJobID { get; set; }
    }

    [Serializable]
    public class ScheduleJobDetails
    {
        public GreenhouseJobScheduler GreenhouseJobScheduler { get; set; }

        public Greenhouse.Data.Model.Core.JobExecutionDetails JED { get; set; }

        public string TimeZoneString { get; set; }

        public string JobServerIP { get; set; }
        public string ServerName { get; set; }
        public string JobType { get; set; }
        public string JobExportName { get; set; }
        public string ServerAlias { get; set; }
        public string SourceName { get; set; }
        public TimeZoneInfo TimeZone
        {
            get
            {
                return TimeZoneInfo.FromSerializedString(this.TimeZoneString);
            }
            set
            {
                this.TimeZoneString = value.ToSerializedString();
            }
        }
        public int SourceID { get; set; }
        //public int SourceJobStepID { get; set; }
    }

    public class DropdownItem
    {
        public string Name { get; set; }

        public Int32 ID { get; set; }
    }
}
using Dapper;
using Greenhouse.Data.Model.Setup;
using System;

namespace Greenhouse.Data.Model.Core
{
    [Serializable]
    public class JobLog : BasePOCO
    {
        public JobLog()
        {
            StartDateTime = DateTime.Now;
            LastUpdatedDateTime = DateTime.Now;
        }

        [Key]
        public Int64 JobLogID { get; set; }
        public int SourceID { get; set; }
        public int IntegrationID { get; set; }
        public int JobCategoryID { get; set; }
        public string JobType { get; set; }
        public string Status { get; set; }
        public string JobDescription { get; set; }
        [IgnoreInsert]
        [IgnoreUpdate]
        public string JobDataJSON { get; set; }
        public Guid JobGUID { get; set; }
        public DateTime StartDateTime { get; set; }
        public DateTime LastUpdatedDateTime { get; set; }
        public string Message { get; set; }
        public string StepDescription { get; set; }

        private JobExecutionDetails _jobData;
        public JobExecutionDetails JobData
        {
            get
            {
                return _jobData;
            }
            set
            {
                _jobData = value;
                if (value != null)
                {
                    JobDataJSON = value.ToJSON();
                }
            }
        }

        public virtual Integration Integration { get; set; }
        public virtual Source Source { get; set; }
    }
}
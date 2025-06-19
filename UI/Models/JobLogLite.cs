namespace Greenhouse.UI.Models
{
    public class JobLogLite
    {
        public Int64 JobLogID { get; set; }
        public string StepDescription { get; set; }
        //For some reason the job framework saves different values in StepDescription & JobDescription 
        //even though they are assigned the SourceJobStep.ShortDescription.
        public string JobDescription { get; set; }
        public string JobStatus { get; set; }
        public string SourceName { get; set; }
        public string IntegrationName { get; set; }
        public string Message { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime StartDateTime { get; set; }
        public string JobGUID { get; set; }

        public int LogCount { get; set; }

        public string ExecutionTime
        {
            get
            {
                var executionTime = JobStatus == "Running" ? DateTime.UtcNow.Subtract(StartDateTime) : LastUpdated.Subtract(StartDateTime);
                return executionTime.ToString(@"d\:h\:mm\:ss\.f");
            }
        }

        public int StatusSortOrder { get; set; }

        public int? FileLogCount { get; set; }

        public string SearchSplunk { get; set; }

        /// <summary>
        /// The new Splunk instance indexes are named after environments.
        /// datalake (For PROD), staging_datalake, qa_datalake, dev_datalake
        /// </summary>
        public string SplunkIndex
        {
            get
            {
                string indexPrefix = string.Empty;
                var environment = Greenhouse.Configuration.Settings.Current.Application.Environment;
                switch (environment)
                {
                    case "LOCALDEV":
                        indexPrefix = "dev";
                        break;
                    case "PROD":
                        break;
                    case "TEST":
                        indexPrefix = "qa";
                        break;
                    default:
                        indexPrefix = environment.ToLower();
                        break;
                }
                string indexName = $"{indexPrefix}_datalake";
                return indexName;
            }
        }
    }
}
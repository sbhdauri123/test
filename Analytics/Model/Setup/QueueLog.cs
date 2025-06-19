using Greenhouse.Configuration;
using System;

namespace Greenhouse.Data.Model.Setup
{
    public class QueueLog
    {
        public string SourceName { get; set; }
        public string IntegrationName { get; set; }
        public string EntityName { get; set; }
        public string EntityID { get; set; }
        public bool IsBackFill { get; set; }
        public string Step { get; set; }
        public string Status { get; set; }
        public DateTime FileDate { get; set; }
        public string FileDateString
        {
            get
            {
                return FileDate.ToString("yyyy-MM-dd");
            }
        }
        public int? FileDateHour { get; set; }
        public DateTime? DeliveryFileDate { get; set; }
        public string DeliveryFileDateString
        {
            get
            {
                return DeliveryFileDate?.ToString("MM/dd/yyyy h:mm tt") ?? "";
            }
        }
        public long? FileSize { get; set; }
        public string FileName { get; set; }
        public Guid? FileGUID { get; set; }

        public string FileGUIDString
        {
            get
            {
                return Convert.ToString(this.FileGUID);
            }
        }

        public DateTime LastUpdated { get; set; }

        public string LastUpdatedString
        {
            get
            {
                return LastUpdated.ToString("MM/dd/yyyy h:mm tt");
            }
        }

        public Guid? JobGUID { get; set; }

        public string JobGUIDString
        {
            get
            {
                return Convert.ToString(this.JobGUID);
            }
        }

        public int StatusSortOrder
        {
            get
            {
                var sortOrder = 0;
                switch (Status)
                {
                    case "Error":
                        sortOrder = 1;
                        break;
                    case "Running":
                        sortOrder = 2;
                        break;
                    case "Pending":
                        sortOrder = 3;
                        break;
                    case "Complete":
                        sortOrder = 4;
                        break;
                }
                return sortOrder;
            }
        }

        public string SplunkSearchFormat { get; set; }

        public string SearchSplunk
        {
            get
            {
                if (String.IsNullOrEmpty(this.SplunkSearchFormat)) return String.Empty;
                return System.Net.WebUtility.UrlEncode(string.Format(this.SplunkSearchFormat, SplunkIndex, JobGUIDString));
            }
        }

        /// <summary>
        /// The new Splunk instance indexes are named after environments.
        /// datalake (For PROD), staging_datalake, qa_datalake, dev_datalake
        /// </summary>
        public static string SplunkIndex
        {
            get
            {
                string indexName = string.Empty;
                var environment = Settings.Current.Application.Environment.ToLower();
                indexName = (environment.Equals("prod", StringComparison.CurrentCultureIgnoreCase)) ? $"datalake" : $"{environment}_datalake";
                return indexName;
            }
        }
    }
}
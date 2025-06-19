using Greenhouse.Data.Model.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Innovid
{
    public class ReportRequest
    {
        public string data { get; set; }
        public string status { get; set; }
    }

    public class ReportStatus
    {
        public string reportUrl { get; set; }
        public string reportStatus { get; set; }
    }

    public class ReportStatusData
    {
        public ReportStatus data { get; set; }
        public string status { get; set; }
    }

    public class ApiReportItem
    {
        public ReportRequestBody ReportRequestBody { get; set; }
        public string ReportType { get; set; }
        public string ReportName { get; set; }
        public DateTime Date { get; set; }

        /// <summary>
        /// Once the report is downloaded, size of the report
        /// </summary>
        public FileCollectionItem FileCollectionItem { get; set; }
        /// <summary>
        /// Once the Import is done, only 1 queue for that day+group will be updated to Import Success
        /// It will contain all the reports for that day+group
        /// </summary>
        public string ReportID { get; set; }
        public bool IsSubmitted { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }
        public DateTime TimeSubmitted { get; set; }
        public Guid FileGuid { get; set; }
        public long QueueID { get; set; }
        public ReportStatusType Status { get; set; }
        public string ReportURL { get; set; }
        public int PriorityNumber { get; set; }
    }

    [Serializable]
    public class ReportRequestBody
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("clients")]
        public List<string> Clients { get; set; }

        [JsonProperty("dateFrom")]

        public string DateFrom { get; set; }

        [JsonProperty("dateTo")]
        public string DateTo { get; set; }

        [JsonProperty("fields")]
        public List<string> Fields { get; set; }
    }
}

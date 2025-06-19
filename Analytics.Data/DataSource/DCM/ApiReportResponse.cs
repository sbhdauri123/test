using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.DCM
{
    [Serializable]
    public class ReportRequestResponse
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }

        [JsonProperty("id")]
        public long ReportID { get; set; }

        [JsonProperty("etag")]
        public string Etag { get; set; }

        [JsonProperty("lastModifiedTime")]
        public string LastModifiedTime { get; set; }

        [JsonProperty("ownerProfileId")]
        public string OwnerProfileID { get; set; }

        [JsonProperty("accountId")]
        public string AccountID { get; set; }

        [JsonProperty("Name")]
        public string Name { get; set; }

        [JsonProperty("type")]
        public string ReportType { get; set; }

        [JsonProperty("criteria")]
        public ReportCriteria ReportCriterias { get; set; }
    }
    public class ReportRunResponse
    {
        public string kind { get; set; }
        public string etag { get; set; }
        public string reportId { get; set; }
        [JsonProperty("id")]
        public string fileId { get; set; }
        public string lastModifiedTime { get; set; }
        public string status { get; set; }
        public string fileName { get; set; }
        public ReportDateRange dateRange { get; set; }
    }

    public class ReportUrl
    {
        public string browserUrl { get; set; }
        public string apiUrl { get; set; }
    }

    public class ReportStatusResponse
    {
        public string kind { get; set; }
        public string etag { get; set; }
        public string reportId { get; set; }
        public string id { get; set; }
        public string lastModifiedTime { get; set; }
        public string status { get; set; }
        public string fileName { get; set; }
        public string format { get; set; }
        public ReportDateRange dateRange { get; set; }
        public ReportUrl urls { get; set; }
    }
}
using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.GoogleAds
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportName")]
        public string ReportName { get; set; }

        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("format")]
        public string FileFormat { get; set; }

        [JsonProperty("extension")]
        public string FileExtension { get; set; }

        [JsonProperty("order")]
        public int Order { get; set; }
        [JsonProperty("hasQueryFilter")]
        public bool HasQueryFilter { get; set; }
        [JsonProperty("hasQuerySegment")]
        public bool HasQuerySegment { get; set; }
        public bool IsStatic
        {
            get { return !this.HasQueryFilter && !this.HasQuerySegment; }
        }
        [JsonProperty("customersNoResultOverride")]
        public bool CustomersNoResultOverride { get; set; }
        [JsonProperty("includeDrafts")]
        public bool IncludeDrafts { get; set; }
        [JsonProperty("callForEntityID")]
        public bool CallForEntityID { get; set; }
    }
}

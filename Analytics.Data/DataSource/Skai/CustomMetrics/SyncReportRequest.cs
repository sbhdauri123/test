using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Skai.CustomMetrics
{
    public class SyncReportRequest
    {
        [JsonProperty("entity")]
        public string Entity { get; set; }
        [JsonProperty("profile_id")]
        public int ProfileId { get; set; }
        [JsonProperty("date_range")]
        public Date_Range DateRange { get; set; }
        [JsonProperty("fields")]
        public CustomMetricField[] Fields { get; set; }
        [JsonProperty("sort")]
        public Sort Sort { get; set; }
        [JsonProperty("limit")]
        public int Limit { get; set; }
        [JsonProperty("page")]
        public int Page { get; set; }
    }

    public class Date_Range
    {
        [JsonProperty("start_date")]
        public string StartDate { get; set; }
        [JsonProperty("end_date")]
        public string EndDate { get; set; }
    }

    public class Sort
    {
        [JsonProperty("field")]
        public string Field { get; set; }
        [JsonProperty("group")]
        public string Group { get; set; }
        [JsonProperty("order")]
        public string Order { get; set; }
    }

    public class CustomMetricField
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("group")]
        public string Group { get; set; }
    }
}

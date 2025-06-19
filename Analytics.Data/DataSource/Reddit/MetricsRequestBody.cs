using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Reddit
{
    public class MetricsRequestBody
    {
        [JsonProperty("data")]
        public ReportData Data { get; set; }
    }

    public class ReportData
    {
        [JsonProperty("breakdowns")]
        public string[] Breakdowns { get; set; }

        [JsonProperty("starts_at")]
        public string StartsAt { get; set; }

        [JsonProperty("ends_at")]
        public string EndsAt { get; set; }

        [JsonProperty("fields")]
        public string[] Fields { get; set; }

        [JsonProperty("time_zone_id")]
        public string TimeZoneID { get; set; }
    }
}

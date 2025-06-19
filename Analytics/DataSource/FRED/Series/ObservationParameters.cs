using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.FRED.Series
{
    public class ObservationParameters : ReportSettings
    {
        [JsonProperty("file_type")]
        public string FileType { get; set; }
        [JsonProperty("series_id")]
        public string SeriesId { get; set; }
        [JsonProperty("realtime_start")]
        public string RealtimeStart { get; set; }
        [JsonProperty("realtime_end")]
        public string RealtimeEnd { get; set; }
        [JsonProperty("limit")]
        public string Limit { get; set; }
        [JsonProperty("offset")]
        public string Offset { get; set; }
        [JsonProperty("sort_order")]
        public string SortOrder { get; set; }
        [JsonProperty("observation_start")]
        public string ObservationStart { get; set; }
        [JsonProperty("observation_end")]
        public string ObservationEnd { get; set; }
        [JsonProperty("units")]
        public string Units { get; set; }
        [JsonProperty("frequency")]
        public string Frequency { get; set; }
        [JsonProperty("aggregation_method")]
        public string AggregationMethod { get; set; }
        [JsonProperty("output_type")]
        public string OutputType { get; set; }
        [JsonProperty("vintage_dates")]
        public string VintageDates { get; set; }
    }
}

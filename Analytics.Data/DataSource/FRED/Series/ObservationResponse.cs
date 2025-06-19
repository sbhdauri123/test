using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.FRED.Series
{
    public class ObservationResponse : Core.ReportResponse
    {
        [JsonProperty("realtime_start")]
        public string RealtimeStart { get; set; }
        [JsonProperty("realtime_end")]
        public string RealtimeEnd { get; set; }
        [JsonProperty("observation_start")]
        public string ObservationStart { get; set; }
        [JsonProperty("observation_end")]
        public string ObservationEnd { get; set; }
        [JsonProperty("units")]
        public string Units { get; set; }
        [JsonProperty("output_type")]
        public string OutputType { get; set; }
        [JsonProperty("file_type")]
        public string FileType { get; set; }
        [JsonProperty("order_by")]
        public string OrderBy { get; set; }
        [JsonProperty("sort_order")]
        public string SortOrder { get; set; }
        [JsonProperty("count")]
        public string Count { get; set; }
        [JsonProperty("offset")]
        public string Offset { get; set; }
        [JsonProperty("limit")]
        public string Limit { get; set; }
        [JsonProperty("observations")]
        public List<ObservationItem> Observations { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class TimeseriesStats
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }
        [JsonProperty("timeseries_stat")]
        public TimeseriesStat TimeseriesStat { get; set; }
    }

    public class TimeseriesStat
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("granularity")]
        public string Granularity { get; set; }
        [JsonProperty("swipe_up_attribution_window")]
        public string SwipeUpAttributionWindow { get; set; }
        [JsonProperty("view_attribution_window")]
        public string ViewAttributionWindow { get; set; }
        [JsonProperty("start_time")]
        public string StartTime { get; set; }
        [JsonProperty("end_time")]
        public string EndTime { get; set; }
        [JsonProperty("finalized_data_end_time")]
        public string FinalizedDataEndTime { get; set; }
        [JsonProperty("timeseries")]
        public List<Timeseries> Timeseries { get; set; }
        [JsonProperty("paging")]
        public Paging Paging { get; set; }
        [JsonProperty("breakdown_stats")]
        public BreakdownStats BreakdownStats { get; set; }
    }
}

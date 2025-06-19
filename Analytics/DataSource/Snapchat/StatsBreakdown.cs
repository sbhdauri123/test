using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class StatsBreakdown
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("granularity")]
        public string Granularity { get; set; }
        [JsonProperty("start_time")]
        public string StartTime { get; set; }
        [JsonProperty("end_time")]
        public string EndTime { get; set; }
        [JsonProperty("timeseries")]
        public List<Timeseries> Timeseries { get; set; }
    }
}

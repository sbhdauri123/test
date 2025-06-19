using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class Timeseries
    {
        [JsonProperty("start_time")]
        public string StartTime { get; set; }
        [JsonProperty("end_time")]
        public string EndTime { get; set; }
        [JsonProperty("stats")]
        public Stats Stats { get; set; }
        [JsonProperty("dimension_stats")]
        public List<Stats> DimensionStats { get; set; }

        [JsonProperty("conversions")]
        public Conversion Conversions { get; set; }
    }
}

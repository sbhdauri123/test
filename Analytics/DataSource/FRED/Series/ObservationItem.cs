using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.FRED.Series
{
    public class ObservationItem
    {
        [JsonProperty("realtime_start")]
        public string RealtimeStart { get; set; }
        [JsonProperty("realtime_end")]
        public string RealtimeEnd { get; set; }
        [JsonProperty("date")]
        public string Date { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
    }
}

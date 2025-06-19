using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class DimensionReport<T>
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }

        public T[] RootObject { get; set; }
    }

    public class StatsReport
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("timeseries_stats")]
        public List<TimeseriesStats> TimeseriesStats { get; set; }
    }

    public class AllData<T>
    {
        public List<T> allData { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.NetBase.Data.MetricValues
{
    public class MetricValuesResponse : Core.ReportResponse
    {
        [JsonProperty("endDate")]
        public string EndDate { get; set; }
        [JsonProperty("errorCode")]
        public string ErrorCode { get; set; }
        [JsonProperty("metrics")]
        public List<Metric> Metrics { get; set; }
        [JsonProperty("startDate")]
        public string StartDate { get; set; }
    }
    public class Metric
    {
        [JsonProperty("timeUnit")]
        public string TimeUnit { get; set; }
        [JsonProperty("columns")]
        public List<string> Columns { get; set; }
        [JsonProperty("dataset")]
        public List<MetricValuesDataset> Dataset { get; set; }
    }
    public class MetricValuesDataset
    {
        [JsonProperty("seriesName")]
        public string SeriesName { get; set; }
        [JsonProperty("set")]
        public List<string> Set { get; set; }
    }
}

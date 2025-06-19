using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.NetBase.Core
{
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public NetBaseApiMethods ReportType { get; set; }
        [JsonProperty("reportMetric")]
        public string ReportMetric { get; set; }
        [JsonProperty("URLPath")]
        public string URLPath { get; set; }

        public enum NetBaseApiMethods
        {
            metricValues,
            insightCount,
            topics,
            themes
        }
    }
}

using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.YouGov
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("url")]
        public string URL { get; set; }

        [JsonProperty("version")]
        public string Version { get; set; }

        [JsonProperty("metrics_score_types")]
        public string MetricsScoreTypes { get; set; }
        [JsonProperty("filters")]
        public string Filters { get; set; }
    }
}

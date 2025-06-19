using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.DCM
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("useMetrics")]
        public bool UseMetrics { get; set; }

        [JsonProperty("useDimensions")]
        public bool UseDimensions { get; set; }

        [JsonProperty("format")]
        public string FileFormat { get; set; }

        [JsonProperty("extension")]
        public string FileExtension { get; set; }

        [JsonProperty("relativeUri")]
        public string RelativeUri { get; set; }
    }
}

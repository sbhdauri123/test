using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.BingAds
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("format")]
        public string FileFormat { get; set; }

        [JsonProperty("extension")]
        public string FileExtension { get; set; }
    }
}

using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Reddit
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("method")]
        public string Method { get; set; }

        [JsonProperty("path")]
        public string Path { get; set; }

        [JsonProperty("deliveryPath")]
        public string DeliveryPath { get; set; }

        [JsonProperty("useMetrics")]
        public bool UseMetrics { get; set; }

        [JsonProperty("useDimensions")]
        public bool UseDimensions { get; set; }

        [JsonProperty("format")]
        public string FileFormat { get; set; }

        [JsonProperty("extension")]
        public string FileExtension { get; set; }

        [JsonProperty("groupBy")]
        public string GroupBy { get; set; }

        [JsonProperty("order")]
        public int Order { get; set; }

        [JsonProperty("cacheKeyValue")]
        public string CacheKeyValue { get; set; }
    }
}

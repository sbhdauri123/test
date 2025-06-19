using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.LinkedIn
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("fileExtension")]
        public string FileExtension { get; set; }

        [JsonProperty("deliveryPath")]
        public string DeliveryPath { get; set; }
    }
}

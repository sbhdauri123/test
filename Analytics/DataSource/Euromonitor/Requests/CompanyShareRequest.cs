using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor.Requests
{
    public class CompanyShareRequest
    {
        public CompanyShareRequest()
        {
            FileType = Euromonitor.FileType.Json;
        }

        [JsonPropertyName("industryCode")]
        public string IndustryCode { get; set; }

        [JsonPropertyName("geographyIds")]
        public int[] GeographyIds { get; set; }

        [JsonPropertyName("companyIds")]
        public int[] CompanyIds { get; set; }

        [JsonPropertyName("shareTypeId")]
        public int ShareTypeId { get; set; }

        [JsonPropertyName("fileType")]
        public int FileType { get; set; }

        [JsonPropertyName("unitType")]
        public int UnitType { get; set; }
    }
}
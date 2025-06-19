using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor.Requests
{
    public class MarketSizeRequest
    {
        public MarketSizeRequest()
        {
            FileType = Euromonitor.FileType.Json;
        }

        [JsonPropertyName("industryCode")]
        public string IndustryCode { get; set; }

        [JsonPropertyName("fileType")]
        public int FileType { get; set; }

        [JsonPropertyName("geographyIds")]
        public IEnumerable<int> GeographyIds { get; set; }
    }
}
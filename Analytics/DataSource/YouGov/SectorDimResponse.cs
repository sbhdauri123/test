using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.YouGov
{
    public class Sector
    {
        [JsonProperty("id")]
        public int ID { get; set; }

        [JsonProperty("region")]
        public string Region { get; set; }

        [JsonProperty("label")]
        public string Label { get; set; }

        [JsonProperty("is_active")]
        public bool IsActive { get; set; }

        [JsonProperty("is_market_scanner")]
        public bool IsMarketScanner { get; set; }
    }

    public class SectorDimResponse
    {
        [JsonProperty("meta")]
        public Meta Meta { get; set; }

        [JsonProperty("data")]
        public Dictionary<string, Sector> Data { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class RegionRoot
    {
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("paging")]
        public Dictionary<string, string> Paging { get; set; }
        [JsonProperty("targeting_dimensions")]
        public RegionTargetingDimensions[] TargetingDimensions { get; set; }
    }

    public partial class RegionTargetingDimensions
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("metro")]
        public RegionInfo RegionInfo { get; set; }
    }

    public partial class RegionInfo
    {
        [JsonProperty("lat")]
        public string Lat { get; set; }

        [JsonProperty("lon")]
        public string Lon { get; set; }

        [JsonProperty("continent")]
        public Continent Continent { get; set; }

        [JsonProperty("country")]
        public Country Country { get; set; }

        [JsonProperty("region")]
        public Region Region { get; set; }
    }

    public partial class Region
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("code")]
        public string Code { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

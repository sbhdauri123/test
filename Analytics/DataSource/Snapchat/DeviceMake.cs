using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class DeviceMakeRoot
    {
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("paging")]
        public Dictionary<string, string> Paging { get; set; }
        [JsonProperty("targeting_dimensions")]
        public DeviceMakeTargetingDimensions[] TargetingDimensions { get; set; }
    }

    public partial class DeviceMakeTargetingDimensions
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("marketing_name")]
        public MarketingName MarketingName { get; set; }
    }

    public partial class MarketingName
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

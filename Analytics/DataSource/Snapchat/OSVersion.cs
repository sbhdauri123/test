using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class OSVersionRoot
    {
        [JsonProperty("request_status")]
        public string Status { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("paging")]
        public Dictionary<string, string> Paging { get; set; }
        [JsonProperty("targeting_dimensions")]
        public OSVersionTargetingDimensions[] TargetingDimensions { get; set; }
    }

    public partial class OSVersionTargetingDimensions
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("os_version")]
        public OSVersion OSVersion { get; set; }
    }

    public partial class OSVersion
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

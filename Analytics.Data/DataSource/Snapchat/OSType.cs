using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class OSTypeRoot
    {
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("paging")]
        public Dictionary<string, string> Paging { get; set; }
        [JsonProperty("targeting_dimensions")]
        public OSTypeTargetingDimensions[] TargetingDimensions { get; set; }
    }

    public partial class OSTypeTargetingDimensions
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("os_type")]
        public OSType OSType { get; set; }
    }

    public partial class OSType
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

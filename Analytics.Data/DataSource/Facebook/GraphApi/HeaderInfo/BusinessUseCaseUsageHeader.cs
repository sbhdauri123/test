using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.HeaderInfo
{
    public class BusinessUseCaseUsageHeader
    {
        public List<BUC> BucInfo { get; set; }
    }

    public class BUC
    {
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("call_count")]
        public int CallCount { get; set; }
        [JsonProperty("total_cputime")]
        public int TotalCputime { get; set; }
        [JsonProperty("total_time")]
        public int TotalTime { get; set; }
        [JsonProperty("estimated_time_to_regain_access")]
        public int EstimatedTimeToRegainAccess { get; set; }
    }
}

using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.HeaderInfo
{
    public class AppUsageHeader
    {
        [JsonProperty("call_count")]
        public string CallsMadePercentage { get; set; }
        [JsonProperty("total_time")]
        public string TotalTimePercentage { get; set; }
        [JsonProperty("total_cputime")]
        public string CpuTimePercentage { get; set; }
    }
}

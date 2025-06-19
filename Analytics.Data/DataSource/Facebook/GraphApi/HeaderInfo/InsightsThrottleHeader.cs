using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.HeaderInfo
{
    public class InsightsThrottleHeader
    {
        [JsonProperty("app_id_util_pct")]
        public string AppIdUtilPct { get; set; }
        [JsonProperty("acc_id_util_pct")]
        public string AccIdUtilPct { get; set; }
    }
}

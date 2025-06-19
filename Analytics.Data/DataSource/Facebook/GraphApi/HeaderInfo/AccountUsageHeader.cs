using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Facebook.GraphApi.HeaderInfo
{
    public class AccountUsageHeader
    {
        [JsonProperty("acc_id_util_pct")]
        public string AccIdUtilPct { get; set; }
    }
}

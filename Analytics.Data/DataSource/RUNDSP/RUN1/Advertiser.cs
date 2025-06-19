using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class Advertiser
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("account_id")]
        public string AccountId { get; set; }
        [JsonProperty("adgear_id")]
        public string AdgearId { get; set; }
        [JsonProperty("adv_id")]
        public string AdvId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

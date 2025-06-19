using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class PmpDeal
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("_type")]
        public string Type { get; set; }
        [JsonProperty("deal_id")]
        public string DealId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("run_deal_id")]
        public string RunDealId { get; set; }
    }
}

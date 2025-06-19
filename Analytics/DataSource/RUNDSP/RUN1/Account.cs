using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class Account
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("account_id")]
        public string AccountId { get; set; }
        [JsonProperty("account_type")]
        public string AccountType { get; set; }
        [JsonProperty("ad_serving_fee")]
        public string AdServingFee { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("plattform_fee")]
        public string PlatformFee { get; set; }
    }
}

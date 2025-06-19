using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class Campaign
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("account_id")]
        public string AccountId { get; set; }
        [JsonProperty("advertiser_id")]
        public string AdvertiserId { get; set; }
        [JsonProperty("campaign_id")]
        public string CampaignId { get; set; }
        [JsonProperty("clearing_cost_enabled")]
        public string ClearingCostEnabled { get; set; }
        [JsonProperty("data_cost_enabled")]
        public string DataCostEnabled { get; set; }
        [JsonProperty("end_at")]
        public string EndAt { get; set; }
        [JsonProperty("ias_cost_enabled")]
        public string IasCostEnabled { get; set; }
        [JsonProperty("ias_qe_cost_enabled")]
        public string IasQeCostEnabled { get; set; }
        [JsonProperty("media_spend")]
        public string MediaSpend { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("run_data_fee_amount")]
        public string RunDataFeeAmount { get; set; }
        [JsonProperty("run_fee_amount")]
        public string RunFeeAmount { get; set; }
        [JsonProperty("start_at")]
        public string StartAt { get; set; }
        [JsonProperty("tms_type")]
        public string TmsType { get; set; }
        [JsonProperty("vendor_cost_enabled")]
        public string VendorCostEnabled { get; set; }
    }
}

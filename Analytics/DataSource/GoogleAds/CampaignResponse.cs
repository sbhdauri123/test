namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    using Newtonsoft.Json;
    using System.Collections.Generic;

    public partial class CampaignResponse
    {
        [JsonProperty("results")]
        public List<CampaignResult> Results { get; set; }

        [JsonProperty("fieldMask")]
        public string FieldMask { get; set; }
    }

    public partial class CampaignResult
    {
        [JsonProperty("customer")]
        public Customer Customer { get; set; }

        [JsonProperty("campaign")]
        public Campaign Campaign { get; set; }

        [JsonProperty("metrics")]
        public Metrics Metrics { get; set; }

        [JsonProperty("segments")]
        public Segments Segments { get; set; }
    }

    public partial class Campaign
    {
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("adServingOptimizationStatus")]
        public string AdServingOptimizationStatus { get; set; }

        [JsonProperty("advertisingChannelType")]
        public string AdvertisingChannelType { get; set; }

        [JsonProperty("networkSettings")]
        public NetworkSettings NetworkSettings { get; set; }

        [JsonProperty("experimentType")]
        public string ExperimentType { get; set; }

        [JsonProperty("servingStatus")]
        public string ServingStatus { get; set; }

        [JsonProperty("biddingStrategyType")]
        public string BiddingStrategyType { get; set; }

        [JsonProperty("frequencyCaps")]
        public List<FrequencyCap> FrequencyCaps { get; set; }

        [JsonProperty("targetCpm")]
        public string TargetCpm { get; set; }

        [JsonProperty("videoBrandSafetySuitability")]
        public string VideoBrandSafetySuitability { get; set; }

        [JsonProperty("geoTargetTypeSetting")]
        public GeoTargetTypeSetting GeoTargetTypeSetting { get; set; }

        [JsonProperty("paymentMode")]
        public string PaymentMode { get; set; }

        [JsonProperty("baseCampaign")]
        public string BaseCampaign { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("campaignBudget")]
        public string CampaignBudget { get; set; }

        [JsonProperty("startDate")]
        public string StartDate { get; set; }

        [JsonProperty("endDate")]
        public string EndDate { get; set; }
    }

    public partial class FrequencyCap
    {
        [JsonProperty("key")]
        public Key Key { get; set; }

        [JsonProperty("cap")]
        public long Cap { get; set; }
    }

    public partial class Key
    {
        [JsonProperty("level")]
        public string Level { get; set; }

        [JsonProperty("timeUnit")]
        public string TimeUnit { get; set; }

        [JsonProperty("eventType")]
        public string EventType { get; set; }

        [JsonProperty("timeLength")]
        public long TimeLength { get; set; }
    }

    public partial class GeoTargetTypeSetting
    {
        [JsonProperty("positiveGeoTargetType")]
        public string PositiveGeoTargetType { get; set; }

        [JsonProperty("negativeGeoTargetType")]
        public string NegativeGeoTargetType { get; set; }
    }

    public partial class NetworkSettings
    {
        [JsonProperty("targetGoogleSearch")]
        public string TargetGoogleSearch { get; set; }

        [JsonProperty("targetSearchNetwork")]
        public string TargetSearchNetwork { get; set; }

        [JsonProperty("targetContentNetwork")]
        public string TargetContentNetwork { get; set; }

        [JsonProperty("targetPartnerSearchNetwork")]
        public string TargetPartnerSearchNetwork { get; set; }
    }

    public partial class TargetCpm
    {
    }

    public partial class Customer
    {
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("descriptiveName")]
        public string DescriptiveName { get; set; }

        [JsonProperty("currencyCode")]
        public string CurrencyCode { get; set; }

        [JsonProperty("timeZone")]
        public string TimeZone { get; set; }
    }
}

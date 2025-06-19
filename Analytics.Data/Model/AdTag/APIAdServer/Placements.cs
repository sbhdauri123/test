using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.AdTag.APIAdServer
{
    public class DimensionValue
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("etag")]
        public string Etag { get; set; }
        [JsonProperty("dimensionName")]
        public string DimensionName { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
    }

    public class Size
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("width")]
        public int Width { get; set; }
        [JsonProperty("height")]
        public int Height { get; set; }
        [JsonProperty("iab")]
        public bool Iab { get; set; }
    }

    public class PricingPeriod
    {
        [JsonProperty("startDate")]
        public string StartDate { get; set; }
        [JsonProperty("endDate")]
        public string EndDate { get; set; }
        [JsonProperty("units")]
        public string Units { get; set; }
        [JsonProperty("rateOrCostNanos")]
        public string RateOrCostNanos { get; set; }
    }

    public class PricingSchedule
    {
        [JsonProperty("testingStartDate")]
        public string TestingStartDate { get; set; }
        [JsonProperty("startDate")]
        public string StartDate { get; set; }
        [JsonProperty("endDate")]
        public string EndDate { get; set; }
        [JsonProperty("pricingType")]
        public string PricingType { get; set; }
        [JsonProperty("capCostOption")]
        public string CapCostOption { get; set; }
        [JsonProperty("disregardOverdelivery")]
        public bool DisregardOverdelivery { get; set; }
        [JsonProperty("flighted")]
        public bool Flighted { get; set; }
        [JsonProperty("pricingPeriods")]
        public List<PricingPeriod> PricingPeriods { get; set; }
    }

    public class TagSetting
    {
        [JsonProperty("additionalKeyValues")]
        public string AdditionalKeyValues { get; set; }
        [JsonProperty("includeClickTracking")]
        public bool IncludeClickTracking { get; set; }
        [JsonProperty("includeClickThroughUrls")]
        public bool IncludeClickThroughUrls { get; set; }
        [JsonProperty("keywordOption")]
        public string KeywordOption { get; set; }
    }

    public class LookbackConfiguration
    {
        [JsonProperty("clickDuration")]
        public int ClickDuration { get; set; }
        [JsonProperty("postImpressionActivitiesDuration")]
        public int PostImpressionActivitiesDuration { get; set; }
    }

    public class TimeInfo
    {
        [JsonProperty("time")]
        public string Time { get; set; }
    }

    public class Placement
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("idDimensionValue")]
        public DimensionValue IdDimensionValue { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("advertiserIdDimensionValue")]
        public DimensionValue AdvertiserIdDimensionValue { get; set; }
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }
        [JsonProperty("campaignIdDimensionValue")]
        public DimensionValue CampaignIdDimensionValue { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("siteId")]
        public string SiteId { get; set; }
        [JsonProperty("siteIdDimensionValue")]
        public DimensionValue SiteIdDimensionValue { get; set; }
        [JsonProperty("keyName")]
        public string KeyName { get; set; }
        [JsonProperty("directorySiteId")]
        public string DirectorySiteId { get; set; }
        [JsonProperty("directorySiteIdDimensionValue")]
        public DimensionValue DirectorySiteIdDimensionValue { get; set; }
        [JsonProperty("paymentSource")]
        public string PaymentSource { get; set; }
        [JsonProperty("compatibility")]
        public string Compatibility { get; set; }
        [JsonProperty("size")]
        public Size Size { get; set; }
        [JsonProperty("archived")]
        public bool Archived { get; set; }
        [JsonProperty("paymentApproved")]
        public bool PaymentApproved { get; set; }
        [JsonProperty("pricingSchedule")]
        public PricingSchedule PricingSchedule { get; set; }
        [JsonProperty("primary")]
        public bool Primary { get; set; }
        [JsonProperty("tagSetting")]
        public TagSetting TagSetting { get; set; }
        [JsonProperty("tagFormats")]
        public List<string> TagFormats { get; set; }
        [JsonProperty("contentCategoryId")]
        public string ContentCategoryId { get; set; }
        [JsonProperty("lookbackConfiguration")]
        public LookbackConfiguration LookbackConfiguration { get; set; }
        [JsonProperty("createInfo")]
        public TimeInfo CreateInfo { get; set; }
        [JsonProperty("lastModifiedInfo")]
        public TimeInfo LastModifiedInfo { get; set; }
        [JsonProperty("sslRequired")]
        public bool SslRequired { get; set; }
        [JsonProperty("videoActiveViewOptOut")]
        public bool VideoActiveViewOptOut { get; set; }
        [JsonProperty("vpaidAdapterChoice")]
        public string VpaidAdapterChoice { get; set; }
        [JsonProperty("adBlockingOptOut")]
        public bool AdBlockingOptOut { get; set; }
        [JsonProperty("subaccountId")]
        public string SubaccountId { get; set; }
        [JsonProperty("comment")]
        public string Comment { get; set; }
    }

    public class PlacementResponse
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("nextPageToken")]
        public string NextPageToken { get; set; }
        [JsonProperty("placements")]
        public List<Placement> Placements { get; set; }
    }
}

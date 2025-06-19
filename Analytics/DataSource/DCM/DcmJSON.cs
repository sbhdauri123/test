using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DCM
{
    public class Ad
    {
        [JsonProperty("id")]
        public string AdId { get; set; }
        [JsonProperty("name")]
        public string AdName { get; set; }
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("startTime")]
        public string StartTime { get; set; }
        [JsonProperty("endTime")]
        public string EndTime { get; set; }
        [JsonProperty("size")]
        public Size Size { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("compatibility")]
        public string Compatibility { get; set; }
    }
    public class Advertiser
    {
        [JsonProperty("id")]
        public string AdvertiserId { get; set; }
        [JsonProperty("name")]
        public string AdvertiserName { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("floodlightConfigurationId")]
        public string FloodlightConfigurationId { get; set; }
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("advertiserGroupId")]
        public string AdvertiserGroupId { get; set; }
    }
    public class Campaign
    {
        [JsonProperty("id")]
        public string CampaignId { get; set; }
        [JsonProperty("name")]
        public string CampaignName { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("advertiserGroupId")]
        public string AdvertiserGroupId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("startDate")]
        public string StartDate { get; set; }
        [JsonProperty("endDate")]
        public string EndDate { get; set; }
    }
    public class Creative
    {
        [JsonProperty("id")]
        public string CreativeId { get; set; }
        [JsonProperty("name")]
        public string CreativeName { get; set; }
        [JsonProperty("renderingId")]
        public string RenderingId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("size")]
        public Size Size { get; set; }
    }
    public class PlacementGroup
    {
        [JsonProperty("id")]
        public string PlacementGroupId { get; set; }
        [JsonProperty("name")]
        public string PlacementGroupName { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }
        [JsonProperty("siteId")]
        public string SiteId { get; set; }
        [JsonProperty("placementGroupType")]
        public string PlacementGroupType { get; set; }
        [JsonProperty("placementStrategyId")]
        public string PlacementStrategyId { get; set; }
        [JsonProperty("pricingSchedule")]
        public PricingSchedule PricingSchedule { get; set; }
    }
    public class Placement
    {
        [JsonProperty("id")]
        public string PlacementId { get; set; }
        [JsonProperty("name")]
        public string PlacementName { get; set; }
        [JsonProperty("accountId")]
        public string AccountId { get; set; }
        [JsonProperty("advertiserId")]
        public string AdvertiserId { get; set; }
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }
        [JsonProperty("siteId")]
        public string SiteId { get; set; }
        [JsonProperty("placementGroupId")]
        public string PlacementGroupId { get; set; }
        [JsonProperty("placementStrategyId")]
        public string PlacementStrategyId { get; set; }
        [JsonProperty("size")]
        public Size Size { get; set; }
        [JsonProperty("pricingSchedule")]
        public PricingSchedule PricingSchedule { get; set; }
    }

    public class AdDimension
    {
        [JsonProperty("ads")]
        public List<Ad> AdCollection { get; set; }
    }
    public class AdvertiserDimension
    {
        [JsonProperty("advertisers")]
        public List<Advertiser> AdvertiserCollection { get; set; }
    }
    public class CampaignDimension
    {
        [JsonProperty("campaigns")]
        public List<Campaign> CampaignCollection { get; set; }
    }
    public class CreativeDimension
    {
        [JsonProperty("creatives")]
        public List<Creative> CreativeCollection { get; set; }
    }
    public class PlacementGroupDimension
    {
        [JsonProperty("placementGroups")]
        public List<PlacementGroup> PlacementGroupCollection { get; set; }
    }
    public class PlacementDimension
    {
        [JsonProperty("placements")]
        public List<Placement> PlacementCollection { get; set; }
    }
    public class Size
    {
        [JsonProperty("id")]
        public string SizeId { get; set; }
        [JsonProperty("width")]
        public string Width { get; set; }
        [JsonProperty("height")]
        public string Height { get; set; }
    }
    public class PricingSchedule
    {
        [JsonProperty("startDate")]
        public string StartDate { get; set; }
        [JsonProperty("endDate")]
        public string EndDate { get; set; }
    }
}
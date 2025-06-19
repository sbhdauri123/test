using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class AdSquadsRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }

        [JsonProperty("adsquads")]
        public AdSquads[] AdSquads { get; set; }
    }

    public partial class AdSquads
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("adsquad")]
        public AdSquad Adsquad { get; set; }
    }

    public partial class AdSquad
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("campaign_id")]
        public string CampaignId { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("targeting")]
        public Targeting Targeting { get; set; }

        [JsonProperty("targeting_reach_status")]
        public string TargetingReachStatus { get; set; }

        [JsonProperty("placement")]
        public string Placement { get; set; }

        [JsonProperty("billing_event")]
        public string BillingEvent { get; set; }

        [JsonProperty("bid_micro", NullValueHandling = NullValueHandling.Ignore)]
        public string BidMicro { get; set; }

        [JsonProperty("auto_bid")]
        public string AutoBid { get; set; }

        [JsonProperty("target_bid")]
        public string TargetBid { get; set; }

        [JsonProperty("bid_strategy")]
        public string BidStrategy { get; set; }

        [JsonProperty("daily_budget_micro", NullValueHandling = NullValueHandling.Ignore)]
        public string DailyBudgetMicro { get; set; }

        [JsonProperty("start_time")]
        public string StartTime { get; set; }

        [JsonProperty("end_time", NullValueHandling = NullValueHandling.Ignore)]
        public string EndTime { get; set; }

        [JsonProperty("optimization_goal")]
        public string OptimizationGoal { get; set; }

        [JsonProperty("delivery_constraint")]
        public string DeliveryConstraint { get; set; }

        [JsonProperty("pacing_type")]
        public string PacingType { get; set; }

        [JsonProperty("conversion_window", NullValueHandling = NullValueHandling.Ignore)]
        public string ConversionWindow { get; set; }

        [JsonProperty("lifetime_budget_micro", NullValueHandling = NullValueHandling.Ignore)]
        public string LifetimeBudgetMicro { get; set; }

        [JsonProperty("pixel_id", NullValueHandling = NullValueHandling.Ignore)]
        public string PixelId { get; set; }
    }

    public partial class Targeting
    {
        [JsonProperty("regulated_content")]
        public string RegulatedContent { get; set; }

        [JsonProperty("demographics", NullValueHandling = NullValueHandling.Ignore)]
        public Demographic[] Demographics { get; set; }

        [JsonProperty("interests", NullValueHandling = NullValueHandling.Ignore)]
        public Interest[] Interests { get; set; }

        [JsonProperty("geos")]
        public Geo[] Geos { get; set; }

        [JsonProperty("devices", NullValueHandling = NullValueHandling.Ignore)]
        public Device[] Devices { get; set; }

        [JsonProperty("segments", NullValueHandling = NullValueHandling.Ignore)]
        public Segment[] Segments { get; set; }

        [JsonProperty("enable_targeting_expansion", NullValueHandling = NullValueHandling.Ignore)]
        public string EnableTargetingExpansion { get; set; }

        [JsonProperty("locations", NullValueHandling = NullValueHandling.Ignore)]
        public Location[] Locations { get; set; }
    }

    public partial class Demographic
    {
        [JsonProperty("min_age", NullValueHandling = NullValueHandling.Ignore)]
        public string MinAge { get; set; }

        [JsonProperty("gender", NullValueHandling = NullValueHandling.Ignore)]
        public string Gender { get; set; }

        [JsonProperty("advanced_demographics", NullValueHandling = NullValueHandling.Ignore)]
        public string[] AdvancedDemographics { get; set; }

        [JsonProperty("languages", NullValueHandling = NullValueHandling.Ignore)]
        public string[] Languages { get; set; }

        [JsonProperty("max_age", NullValueHandling = NullValueHandling.Ignore)]
        public string MaxAge { get; set; }

        [JsonProperty("age_groups", NullValueHandling = NullValueHandling.Ignore)]
        public string[] AgeGroups { get; set; }

        [JsonProperty("operation")]
        public string Operation { get; set; }
    }

    public partial class Device
    {
        [JsonProperty("os_type", NullValueHandling = NullValueHandling.Ignore)]
        public string OsType { get; set; }

        [JsonProperty("marketing_name", NullValueHandling = NullValueHandling.Ignore)]
        public string[] MarketingName { get; set; }

        [JsonProperty("carrier_id", NullValueHandling = NullValueHandling.Ignore)]
        public string[] CarrierId { get; set; }
    }

    public partial class Geo
    {
        [JsonProperty("country_code")]
        public string CountryCode { get; set; }

        [JsonProperty("metro_id", NullValueHandling = NullValueHandling.Ignore)]
        public string[] MetroId { get; set; }

        [JsonProperty("operation", NullValueHandling = NullValueHandling.Ignore)]
        public string Operation { get; set; }

        [JsonProperty("postal_code", NullValueHandling = NullValueHandling.Ignore)]
        public string[] PostalCode { get; set; }
    }

    public partial class Interest
    {
        [JsonProperty("category_id", NullValueHandling = NullValueHandling.Ignore)]
        public string[] CategoryId { get; set; }
    }

    public partial class Location
    {
        [JsonProperty("circles", NullValueHandling = NullValueHandling.Ignore)]
        public Circle[] Circles { get; set; }

        [JsonProperty("operation", NullValueHandling = NullValueHandling.Ignore)]
        public string Operation { get; set; }

        [JsonProperty("location_type", NullValueHandling = NullValueHandling.Ignore)]
        public string[] LocationType { get; set; }

        [JsonProperty("proximity", NullValueHandling = NullValueHandling.Ignore)]
        public string Proximity { get; set; }

        [JsonProperty("proximity_unit", NullValueHandling = NullValueHandling.Ignore)]
        public string ProximityUnit { get; set; }
    }

    public partial class Circle
    {
        [JsonProperty("latitude")]
        public string Latitude { get; set; }

        [JsonProperty("longitude")]
        public string Longitude { get; set; }

        [JsonProperty("radius")]
        public string Radius { get; set; }

        [JsonProperty("unit")]
        public string Unit { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }

    public partial class Segment
    {
        [JsonProperty("segment_id")]
        public string[] SegmentId { get; set; }

        [JsonProperty("operation", NullValueHandling = NullValueHandling.Ignore)]
        public string Operation { get; set; }
    }
}

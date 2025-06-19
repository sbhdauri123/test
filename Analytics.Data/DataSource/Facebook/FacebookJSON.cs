using Greenhouse.Data.DataSource.Facebook.Dimension.AdCreative;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook;

/* Enums */

public enum ReportTypes
{
    AdInsightsReport, //for All data levels
    AdCampaignStatsReport,
    AdSetStatsReport,
    AdStatsReport,
    AdAccount,
    Campaign,
    AdSet,
    Ad,
}

/* UserAdAccounts */
public class DataUserAdAccount
{
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
}

public class UserAdAccounts
{
    public List<DataUserAdAccount> data { get; set; }
    public Paging paging { get; set; }
}

/* ReportStatus */
public class ReportStatus
{
    [JsonProperty("id")]
    public string ReportRunId { get; set; }
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("time_ref")]
    public string TimeRef { get; set; }
    [JsonProperty("time_completed")]
    public string TimeCompleted { get; set; }
    [JsonProperty("async_status")]
    public string AsyncStatus { get; set; }
    [JsonProperty("async_percent_completion")]
    public string AsyncPercentCompletion { get; set; }
}

/* AdInsights StatsReportData */
public class StatsReportData
{
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("account_name")]
    public string AccountName { get; set; }

    [JsonProperty("campaign_id")]
    public string CampaignId { get; set; }
    [JsonProperty("campaign_name")]
    public string CampaignName { get; set; }
    [JsonProperty("adset_id")]
    public string AdSetId { get; set; }
    [JsonProperty("adset_name")]
    public string AdSetName { get; set; }
    [JsonProperty("ad_id")]
    public string AdId { get; set; }
    [JsonProperty("ad_name")]
    public string AdName { get; set; }
    [JsonProperty("buying_type")]
    public string BuyingType { get; set; }
    [JsonProperty("impressions")]
    public string Impressions { get; set; }

    [JsonProperty("clicks")]
    public string Clicks { get; set; }

    [JsonProperty("spend")]
    public string Spend { get; set; }
    [JsonProperty("social_impressions")]
    public string SocialImpressions { get; set; }
    [JsonProperty("social_clicks")]
    public string SocialClicks { get; set; }
    [JsonProperty("reach")]
    public string Reach { get; set; }
    [JsonProperty("unique_clicks")]
    public string UniqueClicks { get; set; }
    [JsonProperty("social_reach")]
    public string SocialReach { get; set; }
    [JsonProperty("social_spend")]
    public string SocialSpend { get; set; }
    [JsonProperty("unique_social_clicks")]
    public string UniqueSocialClicks { get; set; }
    [JsonProperty("date_start")]
    public string DateStart { get; set; }
    [JsonProperty("date_stop")]
    public string DateStop { get; set; }
    [JsonProperty("estimated_ad_recallers")]
    public string EstimatedAdRecallers { get; set; }
    [JsonProperty("frequency")]
    public string Frequency { get; set; }
    //[JsonProperty("age")]
    //public string Age { get; set; }
    //[JsonProperty("gender")]
    //public string Gender { get; set; }
    [JsonProperty("total_actions")]
    public string TotalActions { get; set; }
    [JsonProperty("total_action_value")]
    public string TotalActionValue { get; set; }
    [JsonProperty("total_unique_actions")]
    public string TotalUniqueActions { get; set; }

    //[JsonProperty("unique_impressions")]
    //public string UniqueImpressions { get; set; }

    //[JsonProperty("unique_social_impressions")]
    //public string UniqueSocialImpressions { get; set; }

    //[JsonProperty("deeplink_clicks")]
    //public string DeeplinkClicks { get; set; }
    //[JsonProperty("app_store_clicks")]
    //public string AppStoreClicks { get; set; }
    //[JsonProperty("website_clicks")]
    //public string WebsiteClicks { get; set; }
    [JsonProperty("inline_link_clicks")]
    public string InlineLinkClicks { get; set; }
    [JsonProperty("inline_post_engagement")]
    public string InlinePostEngagement { get; set; }
    //[JsonProperty("card_views")]
    //public string CardViews { get; set; }
    [JsonProperty("call_to_action_clicks")]
    public string CallToActionClicks { get; set; }

    //[JsonProperty("newsfeed_avg_position")]
    //public string NewsfeedAvgPosition { get; set; }

    //[JsonProperty("newsfeed_clicks")]
    //public string NewsfeedClicks { get; set; }

    //[JsonProperty("newsfeed_impressions")]
    //public string NewsfeedImpressions { get; set; }

    [JsonProperty("relevance_score")]
    public string RelevanceScore { get; set; }

    [JsonProperty("actions")]
    public List<StatsReportActions> Actions { get; set; }
    //New
    [JsonProperty("unique_actions")]
    public List<StatsReportActions> UniqueActions { get; set; }

    //[JsonProperty("video_avg_sec_watched_actions")]
    //public List<StatsReportActions> VideoAvgSecWatchedActions { get; set; }

    [JsonProperty("video_avg_time_watched_actions")]
    public List<StatsReportActions> VideoAvgTimeWatchedActions { get; set; }

    //[JsonProperty("video_avg_pct_watched_actions")]
    //public List<StatsReportActions> VideoAvgPctWatchedActions { get; set; }

    [JsonProperty("video_p25_watched_actions")]
    public List<StatsReportActions> VideoP25WatchedActions { get; set; }
    [JsonProperty("video_p50_watched_actions")]
    public List<StatsReportActions> VideoP50WatchedActions { get; set; }
    [JsonProperty("video_p75_watched_actions")]
    public List<StatsReportActions> VideoP75WatchedActions { get; set; }
    [JsonProperty("video_p95_watched_actions")]
    public List<StatsReportActions> VideoP95WatchedActions { get; set; }
    [JsonProperty("video_play_actions")]
    public List<StatsReportActions> VideoPlayActions { get; set; }
    [JsonProperty("conversions")]
    public List<StatsReportActions> Conversions { get; set; }
    [JsonProperty("conversion_values")]
    public List<StatsReportActions> ConversionsValues { get; set; }
    [JsonProperty("video_p100_watched_actions")]
    public List<StatsReportActions> VideoP100WatchedActions { get; set; }

    //[JsonProperty("video_complete_watched_actions")]
    //public List<StatsReportActions> VideoCompleteWatchedActions { get; set; }

    [JsonProperty("video_10_sec_watched_actions")]
    public List<StatsReportActions> Video10SecWatchedActions { get; set; }
    [JsonProperty("video_15_sec_watched_actions")]
    public List<StatsReportActions> Video15SecWatchedActions { get; set; }
    [JsonProperty("video_30_sec_watched_actions")]
    public List<StatsReportActions> Video30SecWatchedActions { get; set; }
    [JsonProperty("action_values")]
    public List<StatsReportActions> ActionValues { get; set; }
    [JsonProperty("publisher_platform")]
    public string PublisherPlatform { get; set; }
    [JsonProperty("platform_position")]
    public string PlatformPosition { get; set; }
    [JsonProperty("device_platform")]
    public string DevicePlatform { get; set; }
    [JsonProperty("impression_device")]
    public string ImpressionDevice { get; set; }
    [JsonProperty("country")]
    public string Country { get; set; }
    [JsonProperty("region")]
    public string Region { get; set; }
    [JsonProperty("dma")]
    public string DMA { get; set; }
    [JsonProperty("video_thruplay_watched_actions")]
    public List<StatsReportActions> VideoThruplayWatchedActions { get; set; }
    [JsonProperty("outbound_clicks")]
    public List<StatsReportActions> OutboundClicks { get; set; }
    [JsonProperty("video_continuous_2_sec_watched_actions")]
    public List<StatsReportActions> VideoContinuous2SecWatchedActions { get; set; }
}

public class StatsReportActions
{
    [JsonProperty("action_type")]
    public string ActionType { get; set; }
    [JsonProperty("action_video_type")]
    public string ActionVideoType { get; set; }
    [JsonProperty("value")]
    public string Value { get; set; }
    [JsonProperty("1d_view")]
    public string Value1dView { get; set; }
    [JsonProperty("1d_click")]
    public string Value1dClick { get; set; }
    [JsonProperty("7d_click")]
    public string Value7dClick { get; set; }
    [JsonProperty("action_reaction")]
    public string ActionReaction { get; set; }
}

public class InsightStatsReport
{
    [JsonProperty("report_run_id")]
    public string ReportRunId { get; set; }
    public List<StatsReportData> data { get; set; }
    public Paging paging { get; set; }
}

// ----------------------------------------
public class BatchResponse
{
    [JsonProperty("code")]
    public int Code { get; set; }
    public List<InsightStatsReportAsyncHeader> headers { get; set; }
    [JsonProperty("body")]
    public string Body { get; set; }
}
public class InsightStatsReportAsyncHeader
{
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("value")]
    public string Value { get; set; }
}
// ----------------------------------------

public class Cursors
{
    public string before { get; set; }
    public string after { get; set; }
}

public class Paging
{
    public Cursors cursors { get; set; }
    public string next { get; set; }
    public string previous { get; set; }
}

/* AdAccountsDimension */
public class TosAcceptedAdAccountDimension
{
    [JsonProperty("web_custom_audience_tos")]
    public string WebCustomAudienceTos { get; set; }
    [JsonProperty("custom_audience_tos")]
    public string CustomAudienceTos { get; set; }
}

public class DataAdAccountDimension
{
    [JsonProperty("name")]
    public string Name { get; set; }
    //private string _permissions;
    //public string permissions { get { return _permissions; } set { _permissions = value; } }
    [JsonProperty("permissions")]
    public List<string> Permissions { get; set; }
    [JsonProperty("role")]
    public string Role { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
}

public class UsersAdAccountDimension
{
    [JsonProperty("data")]
    public List<DataAdAccountDimension> Data { get; set; }
}

public class AdAccountDimension
{
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("account_status")]
    public string AccountStatus { get; set; }
    [JsonProperty("age")]
    public string Age { get; set; }
    [JsonProperty("amount_spent")]
    public string AmountSpent { get; set; }
    [JsonProperty("balance")]
    public string Balance { get; set; }
    [JsonProperty("business_city")]
    public string BusinessCity { get; set; }
    [JsonProperty("business_name")]
    public string BusinessName { get; set; }
    [JsonProperty("business_street2")]
    public string BusinessStreet2 { get; set; }
    [JsonProperty("business_street")]
    public string BusinessStreet { get; set; }
    //private string _capabilities;
    //public string capabilities { get { return _capabilities; } set { _capabilities = String.Join(",", value); } }
    [JsonProperty("capabilities")]
    public List<string> Capabilities { get; set; }
    [JsonProperty("currency")]
    public string Currency { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("is_personal")]
    public string IsPersonal { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("spend_cap")]
    public string SpendCap { get; set; }
    [JsonProperty("timezone_id")]
    public string TimezoneId { get; set; }
    [JsonProperty("timezone_name")]
    public string TimezoneName { get; set; }
    [JsonProperty("timezone_offset_hours_utc")]
    public string TimezoneOffsetHoursUtc { get; set; }
    [JsonProperty("tos_accepted")]
    public TosAcceptedAdAccountDimension TosAccepted { get; set; }
    [JsonProperty("users")]
    public UsersAdAccountDimension Users { get; set; }
    [JsonProperty("tax_id_status")]
    public string TaxIdStatus { get; set; }

    //[JsonProperty("account_groups")]
    //public List<AdAccountGroupAdAccountDimension> AccountGroups { get; set; } //r

    [JsonProperty("business_country_code")]
    public string BusinessCountryCode { get; set; } //r
    [JsonProperty("business_state")]
    public string BusinessState { get; set; } //r
    [JsonProperty("business_zip")]
    public string BusinessZip { get; set; } //r
}

public class AdAccountGroupAdAccountDimension //r
{
    [JsonProperty("account_group_id")]
    public string AccountGroupId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("status")]
    public string Status { get; set; }
}

public class AdCreativeDimensionWithRequestAdId
{
    [JsonProperty("raw_data")]
    public AdCreativeDimension CreativeData { get; set; }
    [JsonProperty("ad_id")]
    public string AdIdInRequest { get; set; }
}

public class AdCreativeDimension
{
    public List<DataAdCreativeDimension> data { get; set; }
    public Paging paging { get; set; }
    public string count { get; set; }
    public string limit { get; set; }
    public string offset { get; set; }
}

public class DataAdCreativeDimension
{
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("body")]
    public string Body { get; set; }
    [JsonProperty("object_type")]
    public string ObjectType { get; set; }
    [JsonProperty("effective_object_story_id")]
    public string EffectiveObjectStoryId { get; set; }
    [JsonProperty("asset_feed_spec")]
    public AssetFeedSpec AssetFeedSpec { get; set; }
    [JsonProperty("object_story_spec")]
    public ObjectStorySpec ObjectStorySpec { get; set; }
}

/* AdCampaignDimension */
public class DataAdCampaignDimension
{
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("objective")]
    public string Objective { get; set; }
    [JsonProperty("effective_status")]
    public string EffectiveStatus { get; set; }
    [JsonProperty("lifetime_budget")]
    public string LifetimeBudget { get; set; }
    [JsonProperty("daily_budget")]
    public string DailyBudget { get; set; }
    [JsonProperty("status")]
    public string Status { get; set; }
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("stop_time")]
    public string StopTime { get; set; }
}

public class AdCampaignDimension
{
    public List<DataAdCampaignDimension> data { get; set; }
    public Paging paging { get; set; }
}

/* AdSetDimension */
public class DataAdSetDimension
{
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("end_time")]
    public string EndTime { get; set; }
    [JsonProperty("daily_budget")]
    public string DailyBudget { get; set; }
    [JsonProperty("effective_status")]
    public string EffectiveStatus { get; set; }
    [JsonProperty("lifetime_budget")]
    public string LifetimeBudget { get; set; }
    [JsonProperty("budget_remaining")]
    public string BudgetRemaining { get; set; }
    [JsonProperty("campaign_id")]
    public string CampaignId { get; set; }

    //Avoids DateTime arithnmetic overflows
    private readonly DateTime MAXDATE = new DateTime(2491, 1, 1);
    private readonly DateTime MINDATE = new DateTime(2000, 1, 1);

    public DateTime StartDateTime { get { return string.IsNullOrEmpty(StartTime) ? MINDATE : DateTime.Parse(StartTime); } }

    public DateTime EndDateTime { get { return string.IsNullOrEmpty(EndTime) ? MAXDATE : DateTime.Parse(EndTime); } }
    [JsonProperty("status")]
    public string Status { get; set; }
}

public class DataAdCreatives
{
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("object_type")]
    public string ObjectType { get; set; }
    [JsonProperty("body")]
    public string Body { get; set; }
    [JsonProperty("effective_object_story_id")]
    public string EffectiveObjectStoryId { get; set; }
}

public class AdSetDimension
{
    public List<DataAdSetDimension> data { get; set; }
    public Paging paging { get; set; }
    public string count { get; set; }
    public string limit { get; set; }
    public string offset { get; set; }
}

public class AdCreatives
{
    public List<DataAdCreatives> data { get; set; }
    public Paging paging { get; set; }
    public string count { get; set; }
    public string limit { get; set; }
    public string offset { get; set; }
}

/* AdDimension */
//https://developers.facebook.com/docs/marketing-api/reference/adgroup/
public class BidInfoAdDimension
{
    [JsonProperty("CLICKS")]
    public string Clicks { get; set; }  //a
    [JsonProperty("ACTIONS")]
    public string Actions { get; set; } //a
    [JsonProperty("IMPRESSIONS")]
    public string Impressions { get; set; }
    [JsonProperty("REACH")]
    public string Reach { get; set; }
    [JsonProperty("SOCIAL")]
    public string Social { get; set; }
}

//https://developers.facebook.com/docs/marketing-api/reference/conversion-action-query/
public class ConversionSpecAdDimension
{
    [JsonProperty("action.type")]
    public List<string> ActionType { get; set; }
    [JsonProperty("application")]
    public List<string> Application { get; set; }
    [JsonProperty("page")]
    public List<string> Page { get; set; }
    [JsonProperty("post")]
    public List<string> Post { get; set; }
    [JsonProperty("event")]
    public List<string> Event { get; set; }
    [JsonProperty("response")]
    public List<string> Response { get; set; }
    [JsonProperty("object")]
    public List<string> Object { get; set; }
    [JsonProperty("offer")]
    public List<string> Offer { get; set; }
    [JsonProperty("offer.creator")]
    public List<string> OfferCreator { get; set; }
    [JsonProperty("post.wall")]
    public List<string> PostWall { get; set; }
    [JsonProperty("conversion_id")]
    public List<string> ConversionId { get; set; }
}

//https://developers.facebook.com/docs/marketing-api/targeting-specs/
public class GeoLocationsAdDimension
{
    [JsonProperty("regions")]
    public List<RegionGeoLocationsAdDimension> Regions { get; set; }
    [JsonProperty("location_types")]
    public List<string> LocationTypes { get; set; }
    [JsonProperty("countries")]
    public List<string> Countries { get; set; }
    [JsonProperty("cities")]
    public List<CityGeoLocationsAdDimension> Cities { get; set; }
    [JsonProperty("zips")]
    public List<ZipGeoLocationsAdDimension> Zips { get; set; }
    [JsonProperty("country_groups")]
    public List<string> CountryGroups { get; set; }
}

public class RegionGeoLocationsAdDimension
{
    [JsonProperty("key")]
    public string RegionKey { get; set; }
    [JsonProperty("name")]
    public string RegionName { get; set; }
    [JsonProperty("country")]
    public string RegionCountry { get; set; }
}

public class CityGeoLocationsAdDimension
{
    [JsonProperty("country")]
    public string Country { get; set; }
    [JsonProperty("distance_unit")]
    public string DistanceUnit { get; set; }
    [JsonProperty("key")]
    public string CityKey { get; set; }
    [JsonProperty("name")]
    public string CityName { get; set; }
    [JsonProperty("radius")]
    public string Radius { get; set; }
    [JsonProperty("region")]
    public string Region { get; set; }
    [JsonProperty("region_id")]
    public string RegionId { get; set; }
}
public class ZipGeoLocationsAdDimension
{
    [JsonProperty("key")]
    public string ZipKey { get; set; }
    [JsonProperty("name")]
    public string ZipName { get; set; }
    [JsonProperty("primary_city_id")]
    public string PrimaryCityId { get; set; }
    [JsonProperty("region_id")]
    public string RegionId { get; set; }
    [JsonProperty("country")]
    public string Country { get; set; }
}

//https://developers.facebook.com/docs/marketing-api/targeting-specs/
public class CategoryTargeting //r
{
    [JsonProperty("id")]
    public string CategoryTargetingId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("parent_category")]
    public string ParentCategory { get; set; }
}

//https://developers.facebook.com/docs/marketing-api/targeting-specs/v2.5
public class TargetingAdDimension : TargetingSegmentAdDimension
{
    [JsonProperty("age_max")]
    public string AgeMax { get; set; }
    [JsonProperty("age_min")]
    public string AgeMin { get; set; }
    [JsonProperty("geo_locations")]
    public GeoLocationsAdDimension GeoLocations { get; set; }
    [JsonProperty("page_types")]
    public List<string> PageTypes { get; set; }
    [JsonProperty("excluded_connections")]
    public List<CategoryTargeting> ExcludedConnections { get; set; }
    [JsonProperty("genders")]
    public List<string> Genders { get; set; }
    [JsonProperty("user_device")]
    public List<string> UserDevice { get; set; }
    [JsonProperty("user_os")]
    public List<string> UserOs { get; set; }
    [JsonProperty("locales")]
    public List<string> Locales { get; set; }
    [JsonProperty("countries")]
    public List<string> Countries { get; set; }
    [JsonProperty("radius")]
    public string Radius { get; set; }
    [JsonProperty("wireless_carrier")]
    public List<string> WirelessCarrier { get; set; }
    [JsonProperty("flexible_spec")]
    public List<TargetingSegmentAdDimension> FlexibleSpec { get; set; }
    [JsonProperty("exclusions")]
    public TargetingSegmentAdDimension Exclusions { get; set; }
    [JsonProperty("excluded_custom_audiences")]
    public List<CategoryTargeting> ExcludedCustomAudiences { get; set; }
    [JsonProperty("dynamic_audience_ids")]
    public List<string> DynamicAudienceIds { get; set; }
    [JsonProperty("excluded_user_device")]
    public List<string> ExcludedUserDevice { get; set; }
    [JsonProperty("device_platforms")]
    public List<string> DevicePlatforms { get; set; }
    [JsonProperty("publisher_platforms")]
    public List<string> PublisherPlatforms { get; set; }
    [JsonProperty("facebook_positions")]
    public List<string> FacebookPositions { get; set; }
    [JsonProperty("instagram_positions")]
    public List<string> InstagramPositions { get; set; }
    [JsonProperty("audience_network_positions")]
    public List<string> AudienceNetworkPositions { get; set; }
    [JsonProperty("messenger_positions")]
    public List<string> MessengerPositions { get; set; }
    [JsonProperty("excluded_publisher_categories")]
    public List<string> ExcludedPublisherCategories { get; set; }
    [JsonProperty("app_install_state")]
    public string AppInstallState { get; set; }
    [JsonProperty("targeting_optimization")]
    public string TargetingOptimization { get; set; }
    [JsonProperty("excluded_geo_locations")]
    public GeoLocationsAdDimension ExcludedGeoLocations { get; set; }
}

public class TargetingSegmentAdDimension
{
    [JsonProperty("connections")]
    public List<CategoryTargeting> Connections { get; set; }
    [JsonProperty("friends_of_connections")]
    public List<CategoryTargeting> FriendsOfConnections { get; set; }
    [JsonProperty("custom_audiences")]
    public List<CategoryTargeting> CustomAudiences { get; set; }
    [JsonProperty("interests")]
    public List<CategoryTargeting> Interests { get; set; }
    [JsonProperty("user_adclusters")]
    public List<CategoryTargeting> UserAdClusters { get; set; }
    [JsonProperty("behaviors")]
    public List<CategoryTargeting> Behaviors { get; set; }
    [JsonProperty("college_years")]
    public List<string> CollegeYears { get; set; }
    [JsonProperty("education_majors")]
    public List<CategoryTargeting> EducationMajors { get; set; }
    [JsonProperty("education_schools")]
    public List<CategoryTargeting> EducationSchools { get; set; }
    [JsonProperty("education_statuses")]
    public List<string> EducationStatuses { get; set; }
    [JsonProperty("family_statuses")]
    public List<CategoryTargeting> FamilyStatuses { get; set; }
    [JsonProperty("generation")]
    public List<CategoryTargeting> Generation { get; set; }
    [JsonProperty("home_type")]
    public List<CategoryTargeting> HomeType { get; set; }
    [JsonProperty("home_ownership")]
    public List<CategoryTargeting> HomeOwnership { get; set; }
    [JsonProperty("home_value")]
    public List<CategoryTargeting> HomeValue { get; set; }
    [JsonProperty("HouseholdComposition")]
    public List<CategoryTargeting> HouseholdComposition { get; set; }
    [JsonProperty("interested_in")]
    public List<CategoryTargeting> InterestedIn { get; set; }
    [JsonProperty("income")]
    public List<CategoryTargeting> Income { get; set; }
    [JsonProperty("industries")]
    public List<CategoryTargeting> Industries { get; set; }
    [JsonProperty("life_events")]
    public List<CategoryTargeting> LifeEvents { get; set; }
    [JsonProperty("moms")]
    public List<CategoryTargeting> Moms { get; set; }
    [JsonProperty("net_worth")]
    public List<CategoryTargeting> NetWorth { get; set; }
    [JsonProperty("office_type")]
    public List<CategoryTargeting> OfficeType { get; set; }
    [JsonProperty("politics")]
    public List<CategoryTargeting> Politics { get; set; }
    [JsonProperty("relationship_statuses")]
    public List<string> RelationshipStatuses { get; set; }
    [JsonProperty("work_positions")]
    public List<CategoryTargeting> WorkPositions { get; set; }
    [JsonProperty("work_employers")]
    public List<CategoryTargeting> WorkEmployers { get; set; }
}

//https://developers.facebook.com/docs/marketing-api/reference/conversion-action-query/
public class TrackingSpecAdDimension
{
    [JsonProperty("action.type")]
    public List<string> ActionType { get; set; }
    [JsonProperty("offsite_pixel")]
    public List<string> OffsitePixel { get; set; }
    [JsonProperty("page")]
    public List<string> Page { get; set; }
    [JsonProperty("post")]
    public List<string> Post { get; set; }
    [JsonProperty("object")]
    public List<string> Object { get; set; }
    [JsonProperty("application")]
    public List<string> Application { get; set; }
    [JsonProperty("event")]
    public List<string> Event { get; set; }
    [JsonProperty("response")]
    public List<string> Response { get; set; }
    [JsonProperty("offer")]
    public List<string> Offer { get; set; }
    [JsonProperty("offer.creator")]
    public List<string> OfferCreator { get; set; }
    [JsonProperty("post.wall")]
    public List<string> PostWall { get; set; }
    [JsonProperty("creative")]
    public List<string> Creative { get; set; }
    [JsonProperty("fb_pixel")]
    public List<string> FbPixel { get; set; }
}

public class DataAdDimension
{
    [JsonProperty("id")]
    public string AdId { get; set; }
    [JsonProperty("account_id")]
    public string AccountId { get; set; }
    [JsonProperty("effective_status")]
    public string EffectiveStatus { get; set; }
    [JsonProperty("bid_type")]
    public string BidType { get; set; }
    [JsonProperty("bid_info")]
    public BidInfoAdDimension BidInfo { get; set; }
    [JsonProperty("campaign_id")]
    public string CampaignId { get; set; }
    [JsonProperty("adset_id")]
    public string AdSetId { get; set; }
    [JsonProperty("conversion_specs")]
    public List<ConversionSpecAdDimension> ConversionSpecs { get; set; }
    [JsonProperty("created_time")]
    public string CreatedTime { get; set; }
    [JsonProperty("last_updated_by_app_id")]
    public string LastUpdatedByAppId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("targeting")]
    public TargetingAdDimension Targeting { get; set; }
    [JsonProperty("tracking_specs")]
    public List<TrackingSpecAdDimension> TrackingSpecs { get; set; }
    [JsonProperty("updated_time")]
    public string UpdatedTime { get; set; }
    [JsonProperty("ad_review_feedback")]
    public AdReviewFeedback AdReviewFeedback { get; set; }
    //[JsonProperty("view_tags")]
    //public List<string> ViewTags { get; set; } //r //Not available
    [JsonProperty("status")]
    public string Status { get; set; }
}

public class AdReviewFeedback
{
    [JsonProperty("global")]
    public Dictionary<string, string> Global { get; set; }
    [JsonProperty("placement_specific")]
    public AdgroupPlacementSpecificReviewFeedback PlacementSpecific { get; set; }
}

public class AdgroupPlacementSpecificReviewFeedback
{
    [JsonProperty("account_admin")]
    public Dictionary<string, string> AccountAdmin { get; set; }

    [JsonProperty("ad")]
    public Dictionary<string, string> Ad { get; set; }

    [JsonProperty("b2c")]
    public Dictionary<string, string> B2C { get; set; }
    [JsonProperty("bsg")]
    public Dictionary<string, string> BSG { get; set; }

    [JsonProperty("city_community")]
    public Dictionary<string, string> CityCommunity { get; set; }

    [JsonProperty("daily_deals")]
    public Dictionary<string, string> DailyDeals { get; set; }

    [JsonProperty("daily_deals_legacy")]
    public Dictionary<string, string> DailyDealsLegacy { get; set; }

    [JsonProperty("dpa")]
    public Dictionary<string, string> DPA { get; set; }

    [JsonProperty("facebook")]
    public Dictionary<string, string> Facebook { get; set; }

    [JsonProperty("instagram")]
    public Dictionary<string, string> Instagram { get; set; }

    [JsonProperty("instagram_shop")]
    public Dictionary<string, string> InstagramShop { get; set; }

    [JsonProperty("marketplace")]
    public Dictionary<string, string> Marketplace { get; set; }

    [JsonProperty("marketplace_home_rentals")]
    public Dictionary<string, string> MarketplaceHomeRentals { get; set; }

    [JsonProperty("marketplace_home_sales")]
    public Dictionary<string, string> MarketplaceHomeSales { get; set; }

    [JsonProperty("marketplace_motors")]
    public Dictionary<string, string> MarketplaceMotors { get; set; }

    [JsonProperty("page_admin")]
    public Dictionary<string, string> PageAdmin { get; set; }

    [JsonProperty("product")]
    public Dictionary<string, string> Product { get; set; }

    [JsonProperty("product_service")]
    public Dictionary<string, string> ProductService { get; set; }

    [JsonProperty("profile")]
    public Dictionary<string, string> Profile { get; set; }

    [JsonProperty("seller")]
    public Dictionary<string, string> Seller { get; set; }

    [JsonProperty("shops")]
    public Dictionary<string, string> Shops { get; set; }

    [JsonProperty("whatsapp")]
    public Dictionary<string, string> Whatsapp { get; set; }
}

public class CursorsAdDimension
{
    public string before { get; set; }
    public string after { get; set; }
}

public class PagingAdDimension
{
    public CursorsAdDimension cursors { get; set; }
    public string next { get; set; }
    public string previous { get; set; }
}

public class AllData<T>
{
    public List<T> allData { get; set; }
}

public class AdDimension
{
    public List<DataAdDimension> data { get; set; }
    public PagingAdDimension paging { get; set; }
    //r
    public string count { get; set; }
    public string limit { get; set; }
    public string offset { get; set; }
}

public class ApiError
{
    [JsonProperty("error")]
    public Error Error { get; set; }
}

public class Error
{
    [JsonProperty("code")]
    public int Code { get; set; }
    [JsonProperty("message")]
    public string Message { get; set; }
}

public class ReportState
{
    [JsonProperty("accountID")]
    public string AccountId { get; set; }

    /// <summary>
    /// Last date the dimension reports were pulled
    /// </summary>
    [JsonProperty("deltaDate")]
    public DateTime? DeltaDate { get; set; }
    /// <summary>
    /// Last date the daily job reports were pulled
    /// </summary>
    [JsonProperty("dailyCompletionDate")]
    public DateTime? DailyCompletionDate { get; set; }
}

public class ErrorSignal
{
    [JsonProperty("retryPageSize")]
    public bool RetryPageSize { get; set; } = false;
    [JsonProperty("message")]
    public string Message { get; set; }
}

/* End */

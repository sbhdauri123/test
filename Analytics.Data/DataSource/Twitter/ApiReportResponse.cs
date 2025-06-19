using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;

namespace Greenhouse.Data.DataSource.Twitter;

[Serializable]
public class ReportRequestResponse
{
    [JsonProperty("request")]
    public ReportRequest Request { get; set; }

    [JsonProperty("next_cursor")]
    public string NextCursor { get; set; }

    [JsonProperty("data")]
    public ReportRequestData Data { get; set; }

    public Dictionary<string, string> Header { get; set; }
    public HttpStatusCode ResponseCode { get; set; }
}

public class ReportRequestStatusResponse
{
    [JsonProperty("request")]
    public ReportRequest Request { get; set; }

    [JsonProperty("next_cursor")]
    public string NextCursor { get; set; }

    [JsonProperty("data")]
    public List<ReportRequestData> Data { get; set; }

    public Dictionary<string, string> Header { get; set; }
    public HttpStatusCode ResponseCode { get; set; }
}

public class ReportRequestData
{
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("segmentation_type")]
    public string SegmentationType { get; set; }
    [JsonProperty("url")]
    public string UrlAddress { get; set; }
    [JsonProperty("entity_ids")]
    public List<string> EntityIds { get; set; }
    [JsonProperty("end_time")]
    public string EndTime { get; set; }
    [JsonProperty("country")]
    public string Country { get; set; }
    [JsonProperty("placement")]
    public string Placement { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("expires_at")]
    public string ExpiresAt { get; set; }
    [JsonProperty("status")]
    public string Status { get; set; }
    [JsonProperty("granularity")]
    public string Granularity { get; set; }
    [JsonProperty("entity")]
    public string Entity { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("platform")]
    public string Platform { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
    [JsonProperty("metric_groups")]
    public List<string> MetricGroups { get; set; }
}

public class ReportRequest
{
    [JsonProperty("params")]
    public ReportParameters Parameters { get; set; }
}

public class ReportParameters
{
    [JsonProperty("account_id")]
    public string AccountID { get; set; }
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("entity_ids")]
    public List<string> EntityIds { get; set; }
    [JsonProperty("end_time")]
    public string EndTime { get; set; }
    [JsonProperty("placement")]
    public string Placement { get; set; }
    [JsonProperty("granularity")]
    public string Granularity { get; set; }
    [JsonProperty("entity")]
    public string Entity { get; set; }
    [JsonProperty("metric_groups")]
    public List<string> MetricGroups { get; set; }
}

public class DimensionReport<T>
{
    [JsonProperty("request")]
    public dynamic Request { get; set; }

    [JsonProperty("next_cursor")]
    public string NextCursor { get; set; }

    [JsonProperty("data")]
    public List<T> Data { get; set; }
}

public class AccountDimensionReport
{
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("business_name")]
    public string BusinessName { get; set; }
    [JsonProperty("timezone")]
    public string TimeZone { get; set; }
    [JsonProperty("timezone_switch_at")]
    public string TimeZoneSwitchAt { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("salt")]
    public string Salt { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
    [JsonProperty("business_id")]
    public string BusinessId { get; set; }
    [JsonProperty("approval_status")]
    public string ApprovalStatus { get; set; }
}

public class CampaignDimensionReport
{
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("reasons_not_servable")]
    public List<string> ReasonsNotServable { get; set; }
    [JsonProperty("servable")]
    public string Servable { get; set; }
    [JsonProperty("daily_budget_amount_local_micro")]
    public string DailyBudgetAmountLocalMicro { get; set; }
    [JsonProperty("end_time")]
    public string EndTime { get; set; }
    [JsonProperty("funding_instrument_id")]
    public string FundingInstrumentId { get; set; }
    [JsonProperty("duration_in_days")]
    public string DurationInDays { get; set; }
    [JsonProperty("standard_delivery")]
    public string StandardDelivery { get; set; }
    [JsonProperty("total_budget_amount_local_micro")]
    public string TotalBudgetAmountLocalMicro { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("entity_status")]
    public string EntityStatus { get; set; }
    [JsonProperty("frequency_cap")]
    public string FrequencyCap { get; set; }
    [JsonProperty("currency")]
    public string Currency { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
}

public class LineItemDimensionReport
{
    [JsonProperty("bid_type")]
    public string BidType { get; set; }
    [JsonProperty("advertiser_user_id")]
    public string AdvertiserUserId { get; set; }
    [JsonProperty("name")]
    public string Name { get; set; }
    [JsonProperty("start_time")]
    public string StartTime { get; set; }
    [JsonProperty("bid_amount_local_micro")]
    public string BidAmountLocalMicro { get; set; }
    [JsonProperty("automatically_select_bid")]
    public string AutomaticallySelectBid { get; set; }
    [JsonProperty("advertiser_domain")]
    public string Advertiser_Domain { get; set; }
    [JsonProperty("target_cpa_local_micro")]
    public string TargetCpaLocalMicro { get; set; }
    [JsonProperty("primary_web_event_tag")]
    public string PrimaryWebEventTag { get; set; }
    [JsonProperty("charge_by")]
    public string ChargeBy { get; set; }
    [JsonProperty("product_type")]
    public string ProductType { get; set; }
    [JsonProperty("end_time")]
    public string EndTime { get; set; }
    [JsonProperty("bid_unit")]
    public string BidUnit { get; set; }
    [JsonProperty("total_budget_amount_local_micro")]
    public string TotalBudgetAmountLocalMicro { get; set; }
    [JsonProperty("objective")]
    public string Objective { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("entity_status")]
    public string EntityStatus { get; set; }
    [JsonProperty("currency")]
    public string Currency { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
    [JsonProperty("include_sentiment")]
    public string IncludeSentiment { get; set; }
    [JsonProperty("campaign_id")]
    public string CampaignId { get; set; }
    [JsonProperty("creative_source")]
    public string CreativeSource { get; set; }
    [JsonProperty("placements")]
    public List<string> Placements { get; set; }
    [JsonProperty("categories")]
    public List<string> Categories { get; set; }
    [JsonProperty("tracking_tags")]
    public List<TrackingTagEntity> TrackingTags { get; set; }
}

public class TrackingTagEntity
{
    [JsonProperty("tracking_partner")]
    public string TrackingPartner { get; set; }

    [JsonProperty("tracking_tag")]
    public string TrackingTag { get; set; }
}

public class PromotedTweetDimensionReport
{
    [JsonProperty("line_item_id")]
    public string LineItemId { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("entity_status")]
    public string EntityStatus { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
    [JsonProperty("approval_status")]
    public string ApprovalStatus { get; set; }
    [JsonProperty("tweet_id")]
    public string TweetId { get; set; }
}

public class MediaCreativeDimensionReport
{
    [JsonProperty("line_item_id")]
    public string LineItemId { get; set; }
    [JsonProperty("landing_url")]
    public string LandingUrl { get; set; }
    [JsonProperty("serving_status")]
    public string ServingStatus { get; set; }
    [JsonProperty("id")]
    public string Id { get; set; }
    [JsonProperty("created_at")]
    public string CreatedAt { get; set; }
    [JsonProperty("account_media_id")]
    public string AccountMediaId { get; set; }
    [JsonProperty("updated_at")]
    public string UpdatedAt { get; set; }
    [JsonProperty("approval_status")]
    public string ApprovalStatus { get; set; }
}
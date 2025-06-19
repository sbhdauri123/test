using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest;

public class DeliveryMetricsPayload
{
    [JsonProperty("attribution_types")]
    public List<string> AttributionTypes { get; set; }
    [JsonProperty("click_window_days")]
    public int ClickWindowDays { get; set; }
    [JsonProperty("conversion_report_time")]
    public string ConversionReportTime { get; set; }
    [JsonProperty("end_date")]
    public string EndDate { get; set; }
    [JsonProperty("engagement_window_days")]
    public int EngagementWindowDays { get; set; }
    [JsonProperty("granularity")]
    public string Granularity { get; set; }
    [JsonProperty("start_date")]
    public string StartDate { get; set; }
    [JsonProperty("view_window_days")]
    public int ViewWindowDays { get; set; }
    [JsonProperty("campaign_ids")]
    public List<string> CampaignIds { get; set; }
    [JsonProperty("campaign_statuses")]
    public List<string> CampaignStatuses { get; set; }
    [JsonProperty("campaign_objective_types")]
    public string CampaignObjectiveTypes { get; set; }
    [JsonProperty("ad_group_ids")]
    public List<string> AdGroupIds { get; set; }
    [JsonProperty("ad_group_statuses")]
    public List<string> AdGroupStatuses { get; set; }
    [JsonProperty("ad_statuses")]
    public List<string> AdStatuses { get; set; }
    [JsonProperty("ad_ids")]
    public List<string> AdIds { get; set; }
    [JsonProperty("pin_promotion_statuses")]
    public string PinPromotionStatuses { get; set; }
    [JsonProperty("product_group_ids")]
    public List<string> ProductGroupIds { get; set; }
    [JsonProperty("product_group_statuses")]
    public string ProductGroupStatuses { get; set; }
    [JsonProperty("product_item_ids")]
    public List<string> ProductItemIds { get; set; }
    [JsonProperty("targeting_types")]
    public List<string> TargetingTypes { get; set; }
    [JsonProperty("metrics_filters")]
    public List<MetricsFilters> MetricsFilters { get; set; }
    [JsonProperty("columns")]
    public List<string> Columns { get; set; }
    [JsonProperty("level")]
    public string Level { get; set; }
    [JsonProperty("report_format")]
    public string ReportFormat { get; set; }
}
public class MetricsFilters
{
    [JsonProperty("field")]
    public string Field { get; set; }
    [JsonProperty("operator")]
    public string Operator { get; set; }
    [JsonProperty("values")]
    public List<int> Values { get; set; }
}

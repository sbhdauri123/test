using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public class AdGroupDimReport
    {
        [JsonProperty("items")]
        public List<AdGroupDim> Items { get; set; }

        // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
        public class AdGroupDim
        {
            [JsonProperty("start_time")]
            public object StartTime { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("placement_group")]
            public string PlacementGroup { get; set; }

            [JsonProperty("end_time")]
            public object EndTime { get; set; }

            [JsonProperty("pacing_delivery_type")]
            public string PacingDeliveryType { get; set; }

            [JsonProperty("campaign_id")]
            public string CampaignId { get; set; }

            [JsonProperty("budget_type")]
            public string BudgetType { get; set; }

            [JsonProperty("auto_targeting_enabled")]
            public bool AutoTargetingEnabled { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("budget_in_micro_currency")]
            public object BudgetInMicroCurrency { get; set; }

            [JsonProperty("tracking_urls")]
            public object TrackingUrls { get; set; }

            [JsonProperty("billable_event")]
            public string BillableEvent { get; set; }

            [JsonProperty("bid_in_micro_currency")]
            public int? BidInMicroCurrency { get; set; }

            [JsonProperty("dca_assets")]
            public object DcaAssets { get; set; }

            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("summary_status")]
            public string SummaryStatus { get; set; }

            [JsonProperty("feed_profile_id")]
            public string FeedProfileId { get; set; }

            [JsonProperty("updated_time")]
            public int UpdatedTime { get; set; }

            [JsonProperty("targeting_spec")]
            public object TargetingSpec { get; set; }

            [JsonProperty("created_time")]
            public int CreatedTime { get; set; }

            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("bid_strategy_type")]
            public string BidStrategyType { get; set; }

            [JsonProperty("ad_account_id")]
            public string AdAccountId { get; set; }

            [JsonProperty("conversion_learning_mode_type")]
            public object ConversionLearningModeType { get; set; }

            [JsonProperty("optimization_goal_metadata")]
            public OptimizationGoalMetadata OptimizationGoalMetadata { get; set; }

            [JsonProperty("lifetime_frequency_cap")]
            public int? LifetimeFrequencyCap { get; set; }
        }

        public class FrequencyGoalMetadata
        {
            [JsonProperty("frequency")]
            public int Frequency { get; set; }

            [JsonProperty("timerange")]
            public string Timerange { get; set; }
        }

        public class OptimizationGoalMetadata
        {
            [JsonProperty("conversion_tag_v3_goal_metadata")]
            public object ConversionTagV3GoalMetadata { get; set; }

            [JsonProperty("frequency_goal_metadata")]
            public FrequencyGoalMetadata FrequencyGoalMetadata { get; set; }

            [JsonProperty("scrollup_goal_metadata")]
            public object ScrollupGoalMetadata { get; set; }

            [JsonProperty("outbound_click_goal_metadata")]
            public object OutboundClickGoalMetadata { get; set; }
        }
    }
}
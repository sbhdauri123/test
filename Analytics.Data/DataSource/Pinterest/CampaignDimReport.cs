using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public class CampaignDimReport
    {
        [JsonProperty("items")]
        public List<Campaign> Items { get; set; }

        [JsonProperty("bookmark")]
        public string Bookmark { get; set; }

        public class Campaign
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("ad_account_id")]
            public string AdAccountId { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("lifetime_spend_cap")]
            public object LifetimeSpendCap { get; set; }

            [JsonProperty("daily_spend_cap")]
            public string DailySpendCap { get; set; }

            [JsonProperty("order_line_id")]
            public string OrderLineId { get; set; }

            [JsonProperty("tracking_urls")]
            public object TrackingUrls { get; set; }

            [JsonProperty("start_time")]
            public string StartTime { get; set; }

            [JsonProperty("end_time")]
            public string EndTime { get; set; }

            [JsonProperty("summary_status")]
            public string SummaryStatus { get; set; }

            [JsonProperty("objective_type")]
            public string ObjectiveType { get; set; }

            [JsonProperty("created_time")]
            public string CreatedTime { get; set; }

            [JsonProperty("updated_time")]
            public string UpdatedTime { get; set; }

            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("is_flexible_daily_budgets")]
            public bool IsFlexibleDailyBudgets { get; set; }

            [JsonProperty("is_campaign_budget_optimization")]
            public bool IsCampaignBudgetOptimization { get; set; }
        }
    }
}
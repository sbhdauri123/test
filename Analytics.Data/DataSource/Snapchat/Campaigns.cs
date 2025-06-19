using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class CampaignsRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }

        [JsonProperty("campaigns")]
        public Campaigns[] Campaigns { get; set; }
    }

    public partial class Campaigns
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("campaign")]
        public Campaign Campaign { get; set; }
    }

    public partial class Campaign
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("ad_account_id")]
        public string AdAccountId { get; set; }

        [JsonProperty("daily_budget_micro")]
        public string DailyBudgetMicro { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("objective")]
        public string Objective { get; set; }

        [JsonProperty("start_time")]
        public string StartTime { get; set; }

        [JsonProperty("end_time")]
        public string EndTime { get; set; }
    }
}

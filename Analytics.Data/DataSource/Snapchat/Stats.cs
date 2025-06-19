using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class Stats
    {
        [JsonProperty("impressions")]
        public string Impressions { get; set; }
        [JsonProperty("swipes")]
        public string Swipes { get; set; }
        [JsonProperty("quartile_1")]
        public string QuartileOne { get; set; }
        [JsonProperty("quartile_2")]
        public string QuartileTwo { get; set; }
        [JsonProperty("quartile_3")]
        public string QuartileThree { get; set; }
        [JsonProperty("view_completion")]
        public string ViewCompletion { get; set; }
        [JsonProperty("attachment_view_completion")]
        public string AttachmentViewCompletion { get; set; }
        [JsonProperty("attachment_total_view_time_millis")]
        public string AttachmentTotalViewTimeMillis { get; set; }
        [JsonProperty("frequency")]
        public string Frequency { get; set; }
        [JsonProperty("avg_view_time_millis")]
        public string AvgViewTimeMillis { get; set; }
        [JsonProperty("attachment_frequency")]
        public string AttachmentFrequency { get; set; }
        [JsonProperty("uniques")]
        public string Uniques { get; set; }
        [JsonProperty("attachment_uniques")]
        public string AttachmentUniques { get; set; }
        [JsonProperty("spend")]
        public string Spend { get; set; }
        [JsonProperty("video_views")]
        public string VideoViews { get; set; }
        [JsonProperty("video_views_time_based")]
        public string VideoViewsTimeBased { get; set; }
        [JsonProperty("video_views_15s")]
        public string VideoViews15s { get; set; }
        [JsonProperty("screen_time_millis")]
        public string ScreenTimeMillis { get; set; }
        [JsonProperty("shares")]
        public string Shares { get; set; }
        [JsonProperty("saves")]
        public string Saves { get; set; }
        [JsonProperty("story_opens")]
        public string StoryOpens { get; set; }
        [JsonProperty("story_completes")]
        public string StoryCompletes { get; set; }
        [JsonProperty("position_impressions")]
        public string PositionImpressions { get; set; }
        [JsonProperty("attachment_video_views")]
        public string AttachmentVideoViews { get; set; }
        [JsonProperty("total_installs")]
        public string TotalInstalls { get; set; }
        [JsonProperty("dma")]
        public string Dma { get; set; }
        [JsonProperty("country")]
        public string Country { get; set; }
        [JsonProperty("make")]
        public string Make { get; set; }
        [JsonProperty("operating_system")]
        public string OperatingSystem { get; set; }
        [JsonProperty("region")]
        public string Region { get; set; }
        [JsonProperty("paid_impressions")]
        public string PaidImpressions { get; set; }
        [JsonProperty("earned_impressions")]
        public string EarnedImpressions { get; set; }
    }
}

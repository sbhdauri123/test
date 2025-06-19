using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest
{
    public class AdsDimReport
    {
        [JsonProperty("items")]
        public List<AdsDim> Items { get; set; }

        public class AdsDim
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("ad_group_id")]
            public string AdGroupId { get; set; }

            [JsonProperty("ad_account_id")]
            public string AdAccountId { get; set; }

            [JsonProperty("android_deep_link")]
            public string AndroidDeepLink { get; set; }

            [JsonProperty("campaign_id")]
            public string CampaignId { get; set; }

            [JsonProperty("carousel_android_deep_links")]
            public List<string> CarouselAndroidDeepLinks { get; set; }

            [JsonProperty("carousel_destination_urls")]
            public List<string> CarouselDestinationUrls { get; set; }

            [JsonProperty("carousel_ios_deep_links")]
            public List<string> CarouselIosDeepLinks { get; set; }

            [JsonProperty("click_tracking_url")]
            public string ClickTrackingUrl { get; set; }

            [JsonProperty("collection_items_destination_url_template")]
            public string CollectionItemsDestinationUrlTemplate { get; set; }

            [JsonProperty("created_time")]
            public long CreatedTimeEpoch { get; set; }

            [JsonProperty("creative_type")]
            public string CreativeType { get; set; }

            [JsonProperty("destination_url")]
            public string DestinationUrl { get; set; }

            [JsonProperty("ios_deep_link")]
            public string IosDeepLink { get; set; }

            [JsonProperty("is_pin_deleted")]
            public bool IsPinDeleted { get; set; }

            [JsonProperty("is_removable")]
            public bool IsRemovable { get; set; }

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("pin_id")]
            public string PinId { get; set; }

            [JsonProperty("rejected_reasons")]
            public List<string> RejectedReasons { get; set; }

            [JsonProperty("rejection_labels")]
            public List<string> RejectionLabels { get; set; }

            [JsonProperty("review_status")]
            public string ReviewStatus { get; set; }

            [JsonProperty("status")]
            public string Status { get; set; }

            [JsonProperty("summary_status")]
            public string SummaryStatus { get; set; }

            [JsonProperty("tracking_urls")]
            public TrackingUrls TrackingUrls { get; set; }

            [JsonProperty("type")]
            public string Type { get; set; }

            [JsonProperty("updated_time")]
            public long UpdatedTimeEpoch { get; set; }

            [JsonProperty("view_tracking_url")]
            public string ViewTrackingUrl { get; set; }

            [JsonProperty("lead_form_id")]
            public string LeadFormId { get; set; }

            [JsonProperty("quiz_pin_data")]
            public QuizPinData QuizPinData { get; set; }

            [JsonProperty("grid_click_type")]
            public string GridClickType { get; set; }

            [JsonProperty("customizable_cta_type")]
            public string CustomizableCtaType { get; set; }

        }
        public class TrackingUrls
        {
            [JsonProperty("impression")]
            public List<string> Impression { get; set; }

            [JsonProperty("click")]
            public List<string> Click { get; set; }

            [JsonProperty("engagement")]
            public List<string> Engagement { get; set; }

            [JsonProperty("buyable_button")]
            public List<string> BuyableButton { get; set; }

            [JsonProperty("audience_verification")]
            public List<string> AudienceVerification { get; set; }
        }

        public class QuizPinData
        {
            [JsonProperty("questions")]
            public List<Question> Questions { get; set; }

            [JsonProperty("results")]
            public List<Result> Results { get; set; }

            [JsonProperty("tie_breaker_type")]
            public string TieBreakerType { get; set; }

            [JsonProperty("tie_breaker_custom_result")]
            public TieBreakerCustomResult TieBreakerCustomResults { get; set; }

        }
        public class Question
        {
            [JsonProperty("question_id")]
            public string QuestionId { get; set; }

            [JsonProperty("question_text")]
            public string QuestionText { get; set; }

            [JsonProperty("options")]
            public List<Option> Options { get; set; }
        }

        public class Option
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("text")]
            public string Text { get; set; }

        }

        public class TieBreakerCustomResult
        {
            [JsonProperty("organic_pin_id")]
            public string OrganicPinId { get; set; }

            [JsonProperty("android_deep_link")]
            public string AndroidDeepLink { get; set; }

            [JsonProperty("ios_deep_link")]
            public string IosDeepLink { get; set; }

            [JsonProperty("destination_url")]
            public string DestinationUrl { get; set; }

            [JsonProperty("result_id")]
            public string ResultId { get; set; }
        }

        public class Result
        {
            [JsonProperty("organic_pin_id")]
            public string OrganicPinId { get; set; }

            [JsonProperty("android_deep_link")]
            public string AndroidDeepLink { get; set; }

            [JsonProperty("ios_deep_link")]
            public string IosDeepLink { get; set; }

            [JsonProperty("destination_url")]
            public string DestinationUrl { get; set; }

            [JsonProperty("result_id")]
            public string ResultId { get; set; }
        }
    }
}
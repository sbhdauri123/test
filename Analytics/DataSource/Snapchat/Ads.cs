using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class AdsRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }

        [JsonProperty("request_id")]
        public string RequestId { get; set; }

        [JsonProperty("ads")]
        public Ads[] Ads { get; set; }

        [JsonProperty("paging")]
        public Paging Paging { get; set; }
    }

    public partial class Ads
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("ad")]
        public Ad Ad { get; set; }
    }

    public partial class Ad
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("updated_at")]
        public string UpdatedAt { get; set; }

        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("ad_squad_id")]
        public string AdSquadId { get; set; }

        [JsonProperty("creative_id")]
        public string CreativeId { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("render_type")]
        public string RenderType { get; set; }

        [JsonProperty("review_status")]
        public string ReviewStatus { get; set; }

        [JsonProperty("third_party_paid_impression_tracking_urls")]
        public ThirdPartyTrackingUrl[] ThirdPartyPaidImpressionTrackingUrls { get; set; }

        //On the 1st of January 2020 we will be sunsetting the fields on_swipe_tracking_urls and third_party_tracking_urls.
        //[JsonProperty("third_party_tracking_urls", NullValueHandling = NullValueHandling.Ignore)]
        //public ThirdPartyTrackingUrl[] ThirdPartyTrackingUrls { get; set; }

        [JsonProperty("paying_advertiser_name")]
        public string PayingAdvertiserName { get; set; }
    }

    public partial class ThirdPartyTrackingUrl
    {
        [JsonProperty("trackingUrlMetadata")]
        public string TrackingUrlMetadata { get; set; }

        [JsonProperty("expandedTrackingUrl")]
        public string ExpandedTrackingUrl { get; set; }

        [JsonProperty("tracking_url")]
        public string TrackingUrl { get; set; }
    }
}

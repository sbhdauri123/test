using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class CreativesRoot
    {
        [JsonProperty("request_status")]
        public string RequestStatus { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
        [JsonProperty("paging")]
        public Dictionary<string, string> Paging { get; set; }
        [JsonProperty("creatives")]
        public Creatives[] Creatives { get; set; }
    }

    public partial class Creatives
    {
        [JsonProperty("sub_request_status")]
        public string SubRequestStatus { get; set; }

        [JsonProperty("creative")]
        public Creative Creative { get; set; }
    }

    public partial class Creative
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
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("packaging_status")]
        public string PackagingStatus { get; set; }
        [JsonProperty("review_status")]
        public string ReviewStatus { get; set; }
        [JsonProperty("shareable")]
        public string Shareable { get; set; }
        [JsonProperty("forced_view_eligibility")]
        public string ForcedViewEligibility { get; set; }
        [JsonProperty("headline")]
        public string Headline { get; set; }
        [JsonProperty("brand_name")]
        public string BrandName { get; set; }
        [JsonProperty("call_to_action")]
        public string CallToAction { get; set; }
        [JsonProperty("render_type")]
        public string RenderType { get; set; }
        [JsonProperty("top_snap_media_id")]
        public string TopSnapMediaId { get; set; }
        [JsonProperty("top_snap_crop_position")]
        public string TopSnapCropPosition { get; set; }
        [JsonProperty("web_view_properties")]
        public WebViewProperties WebViewProperties { get; set; }
        [JsonProperty("ad_product")]
        public string AdProduct { get; set; }
    }
    public partial class WebViewProperties
    {
        [JsonProperty("url")]
        public string Url { get; set; }
        [JsonProperty("allow_snap_javascript_sdk")]
        public string AllowSnapJavascriptSdk { get; set; }
        [JsonProperty("use_immersive_mode")]
        public string UseImmersiveMode { get; set; }
        [JsonProperty("deep_link_urls")]
        public string[] DeepLinkUrls { get; set; }
        [JsonProperty("block_preload")]
        public string BlockPreload { get; set; }
    }
}

namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;

    public partial class AdGrouAdResponse
    {
        [JsonProperty("results")]
        public List<AdGroupAdResult> Results { get; set; }

        [JsonProperty("fieldMask")]
        public string FieldMask { get; set; }
    }

    public partial class AdGroupAdResult
    {
        [JsonProperty("metrics")]
        public Metrics Metrics { get; set; }

        [JsonProperty("adGroupAd")]
        public AdGroupAd AdGroupAd { get; set; }

        [JsonProperty("segments")]
        public Segments Segments { get; set; }
    }

    public partial class AdGroupAd
    {
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("ad")]
        public Ad Ad { get; set; }

        [JsonProperty("policySummary")]
        public PolicySummary PolicySummary { get; set; }

        [JsonProperty("adGroup")]
        public string AdGroup { get; set; }
    }

    public partial class Ad
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("videoAd")]
        public VideoAd VideoAd { get; set; }

        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("finalUrls")]
        public List<Uri> FinalUrls { get; set; }

        [JsonProperty("trackingUrlTemplate")]
        public string TrackingUrlTemplate { get; set; }

        [JsonProperty("displayUrl")]
        public string DisplayUrl { get; set; }

        [JsonProperty("addedByGoogleAds")]
        public string AddedByGoogleAds { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }

    public partial class VideoAd
    {
        [JsonProperty("mediaFile")]
        public string MediaFile { get; set; }
    }

    public partial class PolicySummary
    {
        [JsonProperty("reviewStatus")]
        public string ReviewStatus { get; set; }

        [JsonProperty("approvalStatus")]
        public string ApprovalStatus { get; set; }
    }
}

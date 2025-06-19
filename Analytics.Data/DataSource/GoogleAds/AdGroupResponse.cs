
namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    using Newtonsoft.Json;
    using System.Collections.Generic;

    public partial class AdGroupResponse
    {
        [JsonProperty("results")]
        public List<AdGroupResult> Results { get; set; }

        [JsonProperty("fieldMask")]
        public string FieldMask { get; set; }
    }

    public partial class AdGroupResult
    {
        [JsonProperty("adGroup")]
        public AdGroup AdGroup { get; set; }

        [JsonProperty("metrics")]
        public Metrics Metrics { get; set; }

        [JsonProperty("segments")]
        public Segments Segments { get; set; }
    }

    public partial class AdGroup
    {
        [JsonProperty("resourceName")]
        public string ResourceName { get; set; }

        [JsonProperty("status")]
        public string Status { get; set; }

        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("displayCustomBidDimension")]
        public string DisplayCustomBidDimension { get; set; }

        [JsonProperty("targetingSetting")]
        public TargetingSetting TargetingSetting { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("baseAdGroup")]
        public string BaseAdGroup { get; set; }

        [JsonProperty("campaign")]
        public string Campaign { get; set; }

        [JsonProperty("cpcBidMicros")]

        public string CpcBidMicros { get; set; }

        [JsonProperty("cpmBidMicros")]

        public string CpmBidMicros { get; set; }

        [JsonProperty("targetCpaMicros")]

        public string TargetCpaMicros { get; set; }

        [JsonProperty("cpvBidMicros")]

        public string CpvBidMicros { get; set; }

        [JsonProperty("targetCpmMicros")]

        public string TargetCpmMicros { get; set; }

        [JsonProperty("effectiveTargetCpaMicros")]

        public string EffectiveTargetCpaMicros { get; set; }

        [JsonProperty("explorerAutoOptimizerSetting", NullValueHandling = NullValueHandling.Ignore)]
        public ExplorerAutoOptimizerSetting ExplorerAutoOptimizerSetting { get; set; }
    }

    public partial class ExplorerAutoOptimizerSetting
    {
        [JsonProperty("optIn")]
        public string OptIn { get; set; }
    }

    public partial class TargetingSetting
    {
        [JsonProperty("targetRestrictions")]
        public List<TargetRestriction> TargetRestrictions { get; set; }
    }

    public partial class TargetRestriction
    {
        [JsonProperty("targetingDimension")]
        public string TargetingDimension { get; set; }

        [JsonProperty("bidOnly")]
        public string BidOnly { get; set; }
    }
}
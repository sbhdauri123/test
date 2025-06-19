namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    using Newtonsoft.Json;
    using System.Collections.Generic;

    public partial class Metrics
    {
        [JsonProperty("interactionEventTypes")]
        public List<string> InteractionEventTypes { get; set; }

        [JsonProperty("clicks")]

        public string Clicks { get; set; }

        [JsonProperty("videoQuartileP100Rate")]
        public string VideoQuartileP100Rate { get; set; }

        [JsonProperty("videoQuartileP25Rate")]
        public string VideoQuartileP25Rate { get; set; }

        [JsonProperty("videoQuartileP50Rate")]
        public string VideoQuartileP50Rate { get; set; }

        [JsonProperty("videoQuartileP75Rate")]
        public string VideoQuartileP75Rate { get; set; }

        [JsonProperty("valuePerAllConversions")]
        public string ValuePerAllConversions { get; set; }

        [JsonProperty("valuePerConversion")]
        public string ValuePerConversion { get; set; }

        [JsonProperty("valuePerCurrentModelAttributedConversion")]
        public string ValuePerCurrentModelAttributedConversion { get; set; }

        [JsonProperty("videoViewRate")]
        public string VideoViewRate { get; set; }

        [JsonProperty("videoViews")]

        public string VideoViews { get; set; }

        [JsonProperty("viewThroughConversions")]

        public string ViewThroughConversions { get; set; }

        [JsonProperty("contentBudgetLostImpressionShare", NullValueHandling = NullValueHandling.Ignore)]
        public string ContentBudgetLostImpressionShare { get; set; }

        [JsonProperty("contentImpressionShare", NullValueHandling = NullValueHandling.Ignore)]
        public string ContentImpressionShare { get; set; }

        [JsonProperty("contentRankLostImpressionShare", NullValueHandling = NullValueHandling.Ignore)]
        public string ContentRankLostImpressionShare { get; set; }

        [JsonProperty("conversionsFromInteractionsRate")]
        public string ConversionsFromInteractionsRate { get; set; }

        [JsonProperty("conversionsValue")]
        public string ConversionsValue { get; set; }

        [JsonProperty("conversions")]
        public string Conversions { get; set; }

        [JsonProperty("costMicros")]
        public string CostMicros { get; set; }

        [JsonProperty("costPerAllConversions")]
        public string CostPerAllConversions { get; set; }

        [JsonProperty("costPerConversion")]
        public string CostPerConversion { get; set; }

        [JsonProperty("costPerCurrentModelAttributedConversion")]
        public string CostPerCurrentModelAttributedConversion { get; set; }

        [JsonProperty("crossDeviceConversions")]
        public string CrossDeviceConversions { get; set; }

        [JsonProperty("ctr")]
        public string Ctr { get; set; }

        [JsonProperty("currentModelAttributedConversions")]
        public string CurrentModelAttributedConversions { get; set; }

        [JsonProperty("currentModelAttributedConversionsFromInteractionsRate")]
        public string CurrentModelAttributedConversionsFromInteractionsRate { get; set; }

        [JsonProperty("currentModelAttributedConversionsFromInteractionsValuePerInteraction")]
        public string CurrentModelAttributedConversionsFromInteractionsValuePerInteraction { get; set; }

        [JsonProperty("currentModelAttributedConversionsValue")]
        public string CurrentModelAttributedConversionsValue { get; set; }

        [JsonProperty("currentModelAttributedConversionsValuePerCost")]
        public string CurrentModelAttributedConversionsValuePerCost { get; set; }

        [JsonProperty("engagementRate")]
        public string EngagementRate { get; set; }

        [JsonProperty("engagements")]

        public string Engagements { get; set; }

        [JsonProperty("activeViewCpm")]
        public string ActiveViewCpm { get; set; }

        [JsonProperty("activeViewImpressions")]

        public string ActiveViewImpressions { get; set; }

        [JsonProperty("activeViewMeasurability")]
        public string ActiveViewMeasurability { get; set; }

        [JsonProperty("activeViewMeasurableCostMicros")]

        public string ActiveViewMeasurableCostMicros { get; set; }

        [JsonProperty("activeViewMeasurableImpressions")]

        public string ActiveViewMeasurableImpressions { get; set; }

        [JsonProperty("activeViewViewability")]
        public string ActiveViewViewability { get; set; }

        [JsonProperty("allConversionsFromInteractionsRate")]
        public string AllConversionsFromInteractionsRate { get; set; }

        [JsonProperty("allConversionsValue")]
        public string AllConversionsValue { get; set; }

        [JsonProperty("allConversions")]
        public string AllConversions { get; set; }

        [JsonProperty("averageCost")]
        public string AverageCost { get; set; }

        [JsonProperty("averageCpc", NullValueHandling = NullValueHandling.Ignore)]
        public string AverageCpc { get; set; }

        [JsonProperty("averageCpm")]
        public string AverageCpm { get; set; }

        [JsonProperty("averageCpv")]
        public string AverageCpv { get; set; }

        [JsonProperty("gmailForwards")]

        public string GmailForwards { get; set; }

        [JsonProperty("gmailSaves")]

        public string GmailSaves { get; set; }

        [JsonProperty("gmailSecondaryClicks")]

        public string GmailSecondaryClicks { get; set; }

        [JsonProperty("impressions")]

        public string Impressions { get; set; }

        [JsonProperty("interactionRate")]
        public string InteractionRate { get; set; }

        [JsonProperty("interactions")]

        public string Interactions { get; set; }
    }

    public partial class Segments
    {
        [JsonProperty("device")]
        public string Device { get; set; }

        [JsonProperty("date")]
        public string Date { get; set; }
    }
}
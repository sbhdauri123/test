using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Snapchat
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("entity")]
        public string Entity { get; set; }

        [JsonProperty("breakdown")]
        public string Breakdown { get; set; }

        [JsonProperty("limit")]
        public string Limit { get; set; }

        [JsonProperty("granularity")]
        public string Granularity { get; set; }

        [JsonProperty("omitEmpty")]
        public bool OmitEmpty { get; set; } = true;

        [JsonProperty("reportDimension")]
        public string ReportDimension { get; set; }

        [JsonProperty("conversionSourceTypes")]
        public string ConversionSourceTypes { get; set; }

        [JsonProperty("swipeUpAttributionWindow")]
        public string SwipeUpAttributionWindow { get; set; }

        [JsonProperty("viewAttributionWindow")]
        public string ViewAttributionWindow { get; set; }

        [JsonProperty("parentEntity")]
        public string ParentEntity { get; set; }

        [JsonProperty("URLPath")]
        public string URLPath { get; set; }

        [JsonProperty("parentDataPath")]
        public string ParentDataPath { get; set; }

        [JsonProperty("static")]
        public bool IsStatic { get; set; }
    }

    public class ReportState
    {
        [JsonProperty("accountID")]
        public string AccountId { get; set; }

        /// <summary>
        /// Last date the dimension reports were pulled
        /// </summary>
        [JsonProperty("deltaDate")]
        public DateTime? DeltaDate { get; set; }
    }
}

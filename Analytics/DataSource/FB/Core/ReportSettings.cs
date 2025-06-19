using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.FB.Core
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportName")]
        public string ReportName { get; set; }

        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("level")]
        public string Level { get; set; }

        [JsonProperty("breakdowns")]
        public string Breakdowns { get; set; }

        [JsonProperty("actionBreakdowns")]
        public string ActionBreakdowns { get; set; }

        [JsonProperty("timeIncrement")]
        public string TimeIncrement { get; set; }

        [JsonProperty("limit")]
        public string Limit { get; set; }

        [JsonProperty("dailyStatus")]
        public string DailyStatus { get; set; }

        [JsonProperty("backfillStatus")]
        public string BackfillStatus { get; set; }

        [JsonProperty("url")]
        public string URL { get; set; }
    }
}

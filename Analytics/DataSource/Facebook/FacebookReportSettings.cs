using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Facebook;

[Serializable]
public class FacebookReportSettings
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }
    [JsonProperty("entity")]
    public string Entity { get; set; }

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

    [JsonProperty("attributionWindows")]
    public string AttributionWindows { get; set; }

    [JsonProperty("filtering")]
    public string Filtering { get; set; }

    [JsonProperty("isSummaryReport")]
    public bool IsSummaryReport { get; set; }
    [JsonProperty("datePreset")]
    public string DatePreset { get; set; }
    [JsonProperty("useAdList")]
    public bool UseAdList { get; set; }
    [JsonProperty("reportOrder")]
    public int ReportOrder { get; set; }
}

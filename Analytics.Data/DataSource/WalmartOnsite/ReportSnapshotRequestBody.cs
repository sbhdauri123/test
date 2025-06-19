using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

public class ReportSnapshotRequestBody
{
    [JsonProperty("advertiserId")]
    public int AdvertiserId { get; set; }

    [JsonProperty("startDate")]
    public string StartDate { get; set; }

    [JsonProperty("endDate")]
    public string EndDate { get; set; }

    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("attributionWindow")]
    public string AttributionWindow { get; set; }

    [JsonProperty("format")]
    public string Format { get; set; }

    [JsonProperty("reportMetrics")]
    public List<string> ReportMetrics { get; set; }
}
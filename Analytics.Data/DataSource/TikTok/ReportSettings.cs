using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TikTok;

[Serializable]
public class ReportSettings
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("level")]
    public string Level { get; set; }

    [JsonProperty("method")]
    public string Method { get; set; }

    [JsonProperty("path")]
    public string Path { get; set; }

    [JsonProperty("useMetrics")]
    public bool UseMetrics { get; set; }

    [JsonProperty("useDimensions")]
    public bool UseDimensions { get; set; }

    [JsonProperty("primaryStatus")]
    public string PrimaryStatus { get; set; }

    [JsonProperty("secondaryStatus")]
    public string SecondaryStatus { get; set; }

    [JsonProperty("pageSize")]
    public int PageSize { get; set; }

    [JsonProperty("extension")]
    public string FileExtension { get; set; }

    [JsonProperty("isDimensionReport")]
    public bool IsDimensionReport { get; set; }

    [JsonProperty("isAccountInfo")]
    public bool IsAccountInfo { get; set; } = false;

    [JsonProperty("useSyncApiOnly")]
    public bool UseSyncApiOnly { get; set; } = false;

    [JsonProperty("buying_types")]
    public List<string> BuyingTypes { get; set; } = new();

}

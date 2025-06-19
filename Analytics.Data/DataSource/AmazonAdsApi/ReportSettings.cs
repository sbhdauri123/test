using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.AmazonAdsApi;

[Serializable]
public class ReportSettings
{
    [JsonProperty("order")]
    public int Order { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("useConfiguration")]
    public bool UseConfiguration { get; set; }

    [JsonProperty("adProduct")]
    public string AdProduct { get; set; }

    [JsonProperty("groupBy")]
    public string GroupBy { get; set; }

    [JsonProperty("reportTypeId")]
    public string ReportTypeId { get; set; }

    [JsonProperty("timeUnit")]
    public string TimeUnit { get; set; }

    [JsonProperty("useFilters")]
    public bool UseFilters { get; set; }

    [JsonProperty("filtersfield")]
    public string Filtersfield { get; set; }

    [JsonProperty("format")]
    public string Format { get; set; }
}

public enum ReportType
{
    Dimension,
    Fact,
}

public enum ReportName
{
    Profiles,
    Reporting,
    Advertiser,
}

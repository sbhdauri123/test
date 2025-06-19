using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.AmazonAdsApi.Responses;

public class CreateReportResponse
{
    [JsonProperty("configuration")]
    public Configuration Configuration { get; set; }

    [JsonProperty("createdAt")]
    public string CreatedAt { get; set; }

    [JsonProperty("endDate")]
    public string EndDate { get; set; }

    [JsonProperty("failureReason")]
    public string FailureReason { get; set; }

    [JsonProperty("fileSize")]
    public string FileSize { get; set; }

    [JsonProperty("generatedAt")]
    public string GeneratedAt { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("reportId")]
    public string ReportId { get; set; }

    [JsonProperty("StartDate")]
    public string startDate { get; set; }

    [JsonProperty("status")]
    public string Status { get; set; }

    [JsonProperty("updatedAt")]
    public string UpdatedAt { get; set; }

    [JsonProperty("url")]
    public Uri Url { get; set; }

    [JsonProperty("urlExpiresAt")]
    public string UrlExpiresAt { get; set; }
}

public class Configuration
{
    [JsonProperty("adProduct")]
    public string AdProduct { get; set; }

    [JsonProperty("reportTypeId")]
    public string ReportTypeId { get; set; }

    [JsonProperty("timeUnit")]
    public string TimeUnit { get; set; }
}

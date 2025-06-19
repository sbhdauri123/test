using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Pinterest;

[Serializable]
public class ReportSettings
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("level")]
    public string Level { get; set; }

    [JsonProperty("granularity")]
    public string Granularity { get; set; }

    [JsonProperty("method")]
    public string Method { get; set; }

    [JsonProperty("path")]
    public string Path { get; set; }

    [JsonProperty("deliveryPath")]
    public string DeliveryPath { get; set; }

    [JsonProperty("clickWindowDays")]
    public string ClickWindowDays { get; set; }

    [JsonProperty("engagementWindowDays")]
    public string EngagementWindowDays { get; set; }

    [JsonProperty("viewWindowDays")]
    public string ViewWindowDays { get; set; }

    [JsonProperty("conversionReportTime")]
    public string ConversionReportTime { get; set; }

    [JsonProperty("filters")]
    public List<Filter> Filters { get; set; }

    [JsonProperty("useMetrics")]
    public bool UseMetrics { get; set; }

    [JsonProperty("useDimensions")]
    public bool UseDimensions { get; set; }

    [JsonProperty("format")]
    public string FileFormat { get; set; }

    [JsonProperty("extension")]
    public string FileExtension { get; set; }

    [JsonProperty("queryType")]
    public string QueryType { get; set; }

    [JsonProperty("queryString")]
    public string QueryString { get; set; }

    [JsonProperty("getIDsFrom")]
    public IDsRetrievalInfo GetIDsFrom { get; set; }

    [JsonProperty("entitystatuses")]
    public string Entitystatuses { get; set; }

    [JsonProperty("isDeliveryLocationRegion")]
    public bool IsDeliveryLocationRegion { get; set; }

    [JsonProperty("isDeliveryAppType")]
    public bool IsDeliveryAppType { get; set; }

    [JsonProperty("targeting_types")]
    public List<string> TargetingTypes { get; set; }

    [JsonProperty("campaign_statuses")]
    public List<string> CampaignStatuses { get; set; }

    [JsonProperty("ad_group_statuses")]
    public List<string> AdGroupStatuses { get; set; }

    [JsonProperty("ad_statuses")]
    public List<string> AdStatuses { get; set; }

    public class IDsRetrievalInfo
    {
        [JsonProperty("cacheUniqueKey")]
        public string CacheKey { get; set; }

        [JsonProperty("reportName")]
        public string ReportName { get; set; }

        [JsonProperty("pathDoData")]
        public string PathDoData { get; set; }
    }

    public class Filter
    {
        [JsonProperty("field")]
        public string Field { get; set; }
        [JsonProperty("operator")]
        public string Operator { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
    }
}

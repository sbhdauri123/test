using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Twitter;

[Serializable]
public class ReportSettings
{
    [JsonProperty("reportType")]
    public string ReportType { get; set; }

    [JsonProperty("entity")]
    public string Entity { get; set; }

    [JsonProperty("granularity")]
    public string Granularity { get; set; }

    [JsonProperty("metricGroups")]
    public string MetricGroups { get; set; }

    [JsonProperty("endpoint")]
    public string Endpoint { get; set; }

    [JsonProperty("segmentation")]
    public string Segmentation { get; set; }

    [JsonProperty("entityIdsParamName")]
    public string EntityIdsParamName { get; set; }

    [JsonProperty("tweetType")]
    public string TweetType { get; set; }

    [JsonProperty("isStaticDimension")]
    public bool IsStaticDimension { get; set; }

    [JsonProperty("withDeleted")]
    public bool WithDeleted { get; set; }

    [JsonProperty("includeLegacyCards")]
    public bool IncludeLegacyCards { get; set; }

    [JsonProperty("pageSize")]
    public string PageSize { get; set; }

    [JsonProperty("segmentationValue")]
    public string SegmentationValue { get; set; }

    [JsonProperty("segmentationType")]
    public string SegmentationType { get; set; }

    [JsonProperty("timelineType")]
    public string TimelineType { get; set; }
}
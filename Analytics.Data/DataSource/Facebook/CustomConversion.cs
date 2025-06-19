using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook
{
    public class CustomConversion
    {
        public List<DataCustomConversionDimension> data { get; set; }
        public Paging paging { get; set; }
    }

    public class DataCustomConversionDimension
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("account_id")]
        public string AccountId { get; set; }
        [JsonProperty("creation_time")]
        public string CreationTime { get; set; }
        [JsonProperty("custom_event_type")]
        public string CustomEventType { get; set; }
        [JsonProperty("data_sources")]
        public List<ExternalEventSource> DataSources { get; set; }
        [JsonProperty("default_conversion_value")]
        public string DefaultConversionValue { get; set; }
        [JsonProperty("event_source_type")]
        public string EventSourceType { get; set; }
        [JsonProperty("is_archived")]
        public string IsArchived { get; set; }
        [JsonProperty("is_unavailable")]
        public string IsUnavailable { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("aggregation_rule")]
        public string AggregationRule { get; set; }
        [JsonProperty("business")]
        public BusinessInfo Business { get; set; }
        [JsonProperty("description")]
        public string Description { get; set; }
        [JsonProperty("first_fired_time")]
        public string FirstFiredTime { get; set; }
        [JsonProperty("last_fired_time")]
        public string LastFiredTime { get; set; }
        [JsonProperty("offline_conversion_data_set")]
        public OfflineConversionDataSetInfo OfflineConversionDataSet { get; set; }
        [JsonProperty("retention_days")]
        public string RetentionDays { get; set; }
    }
    public class BusinessInfo
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
    public class OfflineConversionDataSetInfo
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
    public class ExternalEventSource
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("source_type")]
        public string SourceType { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

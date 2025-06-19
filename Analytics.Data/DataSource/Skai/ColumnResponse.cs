using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Skai
{
    public class ColumnResponse
    {
        [JsonProperty("columnsInfo")]
        public ColumnsInfo ColumnsInfo { get; set; }
    }
    public class ColumnsInfo
    {
        [JsonProperty("ChannelCustomerConversionTypes")]
        public List<ColumnAttribute> ChannelCustomerConversionTypes { get; set; }
        [JsonProperty("ProxyCustomerConversionTypes")]
        public List<ColumnAttribute> ProxyCustomerConversionTypes { get; set; }
        [JsonProperty("Attributes")]
        public List<ColumnAttribute> Attributes { get; set; }
        [JsonProperty("Dimensions")]
        public List<ColumnAttribute> Dimensions { get; set; }
        [JsonProperty("CustomMetricsPlus")]
        public List<ColumnAttribute> CustomMetricsPlus { get; set; }
        [JsonProperty("Performance")]
        public List<ColumnAttribute> Performance { get; set; }
        [JsonProperty("ConversionTypes")]
        public List<ColumnAttribute> ConversionTypes { get; set; }
        [JsonProperty("CustomMetrics")]
        public List<ColumnAttribute> CustomMetrics { get; set; }
        [JsonProperty("ExternalCustomerConversionTypes")]
        public List<ColumnAttribute> ExternalCustomerConversionTypes { get; set; }
        [JsonProperty("Custom Solutions")]
        public List<ColumnAttribute> CustomSolutions { get; set; }
        [JsonProperty("AttributionForecasting")]
        public List<ColumnAttribute> AttributionForecasting { get; set; }
    }
    public class ColumnAttribute
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("display_name")]
        public string DisplayName { get; set; }
        [JsonProperty("value_type")]
        public string ValueType { get; set; }
        [JsonProperty("group")]
        public string Group { get; set; }
    }
}

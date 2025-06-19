using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class ResourceParameters
    {
        [JsonProperty("type"), JsonConverter(typeof(StringEnumConverter))]
        public ReportType Type { get; set; }
        [JsonProperty("groupBys")]
        public List<string> GroupBys { get; set; }
        [JsonProperty("filters")]
        public List<Filter> Filters { get; set; }
        [JsonProperty("metrics")]
        public List<string> Metrics { get; set; }
    }
    public class Filter
    {
        [JsonProperty("type"), JsonConverter(typeof(StringEnumConverter))]
        public FilterType Type { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
    }
}

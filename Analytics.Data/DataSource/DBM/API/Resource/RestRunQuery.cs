using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class RestRunQuery
    {
        [JsonProperty("dataRange")]
        public RunDataRange DataRange { get; set; }
    }

    public class RunDataRange
    {
        [JsonProperty("range"), JsonConverter(typeof(StringEnumConverter))]
        public DataRange Range { get; set; } = DataRange.CUSTOM_DATES;
        [JsonProperty("customStartDate")]
        public ResourceDate CustomStartDate { get; set; }
        [JsonProperty("customEndDate")]
        public ResourceDate CustomEndDate { get; set; }
    }
}
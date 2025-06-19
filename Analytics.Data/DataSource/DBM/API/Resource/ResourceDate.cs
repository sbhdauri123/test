using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class ResourceDate
    {
        [JsonProperty("year")]
        public int Year { get; set; }
        [JsonProperty("month")]
        public int Month { get; set; }
        [JsonProperty("day")]
        public int Day { get; set; }
    }
}

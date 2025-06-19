using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class GeoShape
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("country")]
        public string Country { get; set; }
        [JsonProperty("display_name")]
        public string DisplayName { get; set; }
        [JsonProperty("geo_id")]
        public string GeoId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("type")]
        public string Type { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Innovid
{
    public class WeightConfiguration
    {
        [JsonProperty("weight")]
        public int Weight { get; set; }
        [JsonProperty("bundles")]
        public List<string> Bundles { get; set; }
        [JsonProperty("isDefault")]
        public bool IsDefault { get; set; }
    }
}

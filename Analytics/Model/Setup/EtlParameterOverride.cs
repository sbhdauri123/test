using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class EtlParameterOverride
    {
        [JsonProperty("level")]
        public string OverrideType { get; set; }

        [JsonProperty("parameter")]
        public string ParameterName { get; set; }

        [JsonProperty("defaultValue")]
        public string ReplacementValue { get; set; }

        [JsonProperty("id")]
        public string OverrideTypeId { get; set; }
    }
}

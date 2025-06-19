using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class IntegrationAggregateSettings
    {
        [JsonProperty("useForManualBackFill")]
        public bool UseForManualBackFill { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Lotame
{
    public class BehaviorTypes
    {
        [JsonProperty("behaviorType")]
        public List<BehaviorType> BehaviorType { get; set; }
    }

    public class BehaviorType
    {
        [JsonProperty("id")]
        public string Id { get; set; }
    }
}
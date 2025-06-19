using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Lotame
{
    public class BehaviorSearchResponse
    {
        [JsonProperty("setInfo")]
        public SetInfo SetInfo { get; set; }

        [JsonProperty("behaviors")]
        public Behaviors Behaviors { get; set; }
    }

    public class SetInfo
    {
        [JsonProperty("numAvailable")]
        public int TotalCount { get; set; }

        [JsonProperty("numProvided")]
        public int Records { get; set; }
    }

    public class Behaviors
    {
        [JsonProperty("behavior")]
        public List<Behavior> Behavior { get; set; }
    }

    public class Behavior
    {
        [JsonProperty("id")]
        public int Id { get; set; }
    }
}

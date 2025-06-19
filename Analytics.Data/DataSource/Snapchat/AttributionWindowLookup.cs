using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class AttributionWindowLookup
    {
        [JsonProperty("entity_id")]
        public string EntityID { get; set; }
        [JsonProperty("windows")]
        public List<AttributionWindow> Windows { get; set; }
    }
    public class AttributionWindow
    {
        [JsonProperty("swipe")]
        public string SwipeWindow { get; set; }
        [JsonProperty("view")]
        public string ViewWindow { get; set; }
    }
}

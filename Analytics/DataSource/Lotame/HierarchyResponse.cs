using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Lotame
{
    public class HierarchyResponse
    {
        [JsonProperty("hierarchies")]
        public IEnumerable<Hierarchy> Hierarchies { get; set; }
    }

    public class Hierarchy
    {
        [JsonProperty("id")]
        public long Id { get; set; }
    }
}
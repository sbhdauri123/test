using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Lotame
{
    public class HierarchyDetail
    {
        [JsonProperty("nodes")]
        public List<Node> Nodes { get; set; }
    }

    public class Node
    {
        [JsonProperty("id")]
        public int Id { get; set; }
    }
}
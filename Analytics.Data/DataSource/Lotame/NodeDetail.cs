using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Lotame
{
    public class NodeDetail
    {
        [JsonProperty("childNodes")]
        public List<Node> ChildNodes { get; set; }
    }
}
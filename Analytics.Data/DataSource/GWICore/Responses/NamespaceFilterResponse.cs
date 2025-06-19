using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    public class NamespaceFilterResponse
    {
        [JsonProperty("namespaces")]
        public List<Namespace> Namespaces { get; set; }
    }


}

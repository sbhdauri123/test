using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    /// <summary>
    ///     SplitterFilterRequest.
    /// </summary>
    public class SplitterFilterRequest
    {
        /// <summary>
        ///     Initialize a new instance of the <see cref="SplitterFilterRequest" /> class.
        /// </summary>
        public SplitterFilterRequest()
        {
            Splitters = new List<Splitter>();
        }

        [JsonProperty("splitters")]
        public List<Splitter> Splitters { get; set; }
    }

    public class Splitter
    {
        [JsonProperty("namespace_code")]
        public string NamespaceCode { get; set; }
    }
}
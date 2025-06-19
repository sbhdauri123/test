using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.GWICore.Requests
{
    /// <summary>
    ///     WavesFilterRequest.
    /// </summary>
    public class WavesFilterRequest
    {
        /// <summary>
        ///     Initialize a new instance of the <see cref="WavesFilterRequest" /> class.
        /// </summary>
        public WavesFilterRequest()
        {
            Namespaces = new List<Namespace>();
        }

        [JsonProperty("namespaces")]
        public List<Namespace> Namespaces { get; set; }
    }
}
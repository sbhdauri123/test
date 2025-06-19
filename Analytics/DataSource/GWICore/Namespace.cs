using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.GWICore
{
    /// <summary>
    ///     Namespace.
    /// </summary>
    public class Namespace
    {
        [JsonProperty("code")]
        public string Code { get; set; }
    }
}
using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Brief
{
    public class DataManagerWebError
    {
        [JsonProperty("httpStatusCode")]
        public int HttpStatusCode { get; set; }
        [JsonProperty("retry")]
        public bool Retry { get; set; } = false;
    }
}

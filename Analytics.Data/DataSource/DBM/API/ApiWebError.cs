using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.DBM.API
{
    public class ApiWebError
    {
        [JsonProperty("httpStatusCode")]
        public int HttpStatusCode { get; set; }
        [JsonProperty("retry")]
        public bool Retry { get; set; } = false;
    }
}
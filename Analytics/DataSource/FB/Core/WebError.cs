using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.FB.Core
{
    public class WebError
    {
        [JsonProperty("httpStatusCode")]
        public int HttpStatusCode { get; set; }
        [JsonProperty("retry")]
        public bool Retry { get; set; } = false;
    }
}

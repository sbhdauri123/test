using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Facebook
{
    public class FacebookBackoff
    {
        [JsonProperty("counter")]
        public int Counter { get; set; }

        [JsonProperty("maxRetry")]
        public int MaxRetry { get; set; }
    }
}

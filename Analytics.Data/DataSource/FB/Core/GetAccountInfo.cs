using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.FB.Core
{
    public class GetAccountInfo
    {
        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

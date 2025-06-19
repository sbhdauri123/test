using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.YouGov
{
    public class AuthenticationRequest
    {
        [JsonProperty("data")]
        public Data Data { get; set; }

        [JsonProperty("meta")]
        public Meta Meta { get; set; }
    }

    public class Data
    {
        [JsonProperty("email")]
        public string Email { get; set; }
        [JsonProperty("password")]
        public string Password { get; set; }
    }

    public class Meta
    {
        [JsonProperty("version")]
        public string Version { get; set; }
    }
}

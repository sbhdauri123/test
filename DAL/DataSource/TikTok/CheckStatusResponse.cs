using Newtonsoft.Json;


namespace Greenhouse.DAL.DataSource.TikTok
{
    public class CheckStatusResponse
    {
        [JsonProperty("code")]
        public int Code { get; set; }

        [JsonProperty("data")]
        public CheckStatusData Data { get; set; }
    }

    public class CheckStatusData
    {
        [JsonProperty("status")]
        public string Status { get; set; }
    }
}

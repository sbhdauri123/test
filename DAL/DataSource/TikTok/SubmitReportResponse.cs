using Newtonsoft.Json;

namespace Greenhouse.DAL.DataSource.TikTok
{
    public class SubmitReportResponse
    {
        [JsonProperty("code")]
        public int Code { get; set; }

        [JsonProperty("data")]
        public Data Data { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }
    }

    public class Data
    {
        [JsonProperty("task_id")]
        public string TaskId { get; set; }
    }
}

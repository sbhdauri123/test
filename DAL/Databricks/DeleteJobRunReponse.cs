using Newtonsoft.Json;

namespace Greenhouse.DAL.Databricks.RunListResponse
{
    public class DeleteJobRunResponse
    {
        [JsonProperty("error_code")]
        public string ErrorCode { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }
    }
}
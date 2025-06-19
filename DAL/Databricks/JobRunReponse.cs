using Newtonsoft.Json;

namespace Greenhouse.DAL.Databricks.RunListResponse
{
    public class JobRunResponse
    {
        [JsonProperty("run_id")]
        public long RunID { get; set; }
    }
}
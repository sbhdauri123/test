using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Skai.CustomMetrics
{
    public class SyncReportResponse
    {
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("paging")]
        public Paging Paging { get; set; }
    }

    public class Paging
    {
        [JsonProperty("previous_page")]
        public string PreviousPage { get; set; }
        [JsonProperty("next_page")]
        public string NextPage { get; set; }
    }
}

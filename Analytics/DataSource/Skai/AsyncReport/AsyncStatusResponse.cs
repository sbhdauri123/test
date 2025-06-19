using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Skai
{
    [Serializable]
    public class AsyncReportResponse
    {
        [JsonProperty("run_id")]
        public string RunId { get; set; }
    }

    [Serializable]
    public class PollResponse
    {
        [JsonProperty("status")]
        public string Status { get; set; }
    }
}
using Newtonsoft.Json;
using System;

namespace Greenhouse.DAL.Databricks
{
    [Serializable]
    public class RepairJobRunRequest
    {
        [JsonProperty("run_id")]
        public string JobRunID { get; set; }

        [JsonProperty("rerun_all_failed_tasks")]
        public bool RerunAllFailedTasks { get; set; } = true;

        [JsonProperty("latest_repair_id")]
        public string LatestRepairID { get; set; }

        [JsonProperty("jar_params")]
        public string[] JarParams { get; set; }
    }
}

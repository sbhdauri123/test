using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.Databricks
{
    [Serializable]
    public class JobRunRequest
    {
        [JsonProperty("job_id")]
        public long JobID { get; set; }
        [JsonProperty("jar_params")]
        public string[] JarParams { get; set; }
        [JsonProperty("notebook_params")]
        public Dictionary<string, string> NotebookParams { get; set; }
        [JsonProperty("python_params")]
        public string[] PythonParams { get; set; }
        [JsonProperty("spark_submit_params")]
        public string[] SparkSubmitParams { get; set; }
        [JsonProperty("python_named_params")]
        public Dictionary<string, string> PythonNamedParams { get; set; }
        [JsonProperty("sql_params")]
        public Dictionary<string, string> SqlParams { get; set; }
        [JsonProperty("dbt_commands")]
        public string[] DbtCommands { get; set; }
        [JsonProperty("pipeline_params")]
        public Dictionary<string, string> PipelineParams { get; set; }
        [JsonProperty("idempotency_token")]
        public string IdempotencyToken { get; set; }
        [JsonProperty("queue")]
        public JobQueueSettings DatabricksJobQueue { get; set; }
        [JsonProperty("job_parameters")]
        public Dictionary<string, string> JobParameters { get; set; }
    }

    public class JobQueueSettings
    {
        [JsonProperty("enabled")]
        public bool Enabled { get; set; }
    }
}

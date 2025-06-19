
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.Databricks.RunListResponse
{
    public class JobListResponse
    {
        [JsonProperty("runs")]
        public List<Run> Runs { get; set; }

        [JsonProperty("has_more")]
        public bool HasMore { get; set; }
    }

    public class Autoscale
    {
        [JsonProperty("min_workers")]
        public int MinWorker { get; set; }

        [JsonProperty("max_workers")]
        public int MaxWorker { get; set; }
    }

    public class AwsAttributes
    {
        [JsonProperty("availability")]
        public string Availability { get; set; }

        [JsonProperty("zone_id")]
        public string ZoneID { get; set; }
    }

    public class ClusterInstance
    {
        [JsonProperty("cluster_id")]
        public string ClusterId { get; set; }

        [JsonProperty("spark_context_id")]
        public string SparkContextId { get; set; }
    }

    public class ClusterSpec
    {
        [JsonProperty("existing_cluster_id")]
        public string ExistingClusterId { get; set; }

        [JsonProperty("new_cluster")]
        public NewCluster NewCluster { get; set; }

        [JsonProperty("libraries")]
        public dynamic Libraries { get; set; }
    }

    public class JobCluster
    {
        [JsonProperty("job_cluster_key")]
        public string JobClusterKey { get; set; }

        [JsonProperty("new_cluster")]
        public NewCluster NewCluster { get; set; }
    }

    public class Library
    {
    }

    public class NewCluster
    {
        [JsonProperty("num_workers")]
        public int NumWorkers { get; set; }

        [JsonProperty("autoscale")]
        public Autoscale Autoscale { get; set; }

        [JsonProperty("spark_version")]
        public string SparkVersion { get; set; }

        [JsonProperty("spark_conf")]
        public SparkConf SparkConf { get; set; }

        [JsonProperty("aws_attributes")]
        public AwsAttributes AwsAttributes { get; set; }

        [JsonProperty("node_type_id")]
        public string NodeTypeId { get; set; }

        [JsonProperty("driver_node_type_id")]
        public string DriverNodeTypeId { get; set; }

        [JsonProperty("ssh_public_keys")]
        public List<object> SshPublicKeys { get; set; }

        [JsonProperty("custom_tags")]
        public dynamic CustomTags { get; set; }

        [JsonProperty("cluster_log_conf")]
        public dynamic ClusterLogConf { get; set; }

        [JsonProperty("init_scripts")]
        public List<object> InitScripts { get; set; }

        [JsonProperty("spark_env_vars")]
        public SparkEnvVars SparkEnvVars { get; set; }

        [JsonProperty("enable_elastic_disk")]
        public bool EnableElasticDisk { get; set; }

        [JsonProperty("driver_instance_pool_id")]
        public string DriverInstancePoolId { get; set; }

        [JsonProperty("instance_pool_id")]
        public string InstancePoolId { get; set; }

        [JsonProperty("policy_id")]
        public string PolicyId { get; set; }
    }

    public class NotebookParams
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("age")]
        public string Age { get; set; }
    }

    public class NotebookTask
    {
    }

    public class OverridingParameters
    {
        [JsonProperty("jar_params")]
        public List<string> JarParams { get; set; }

        [JsonProperty("notebook_params")]
        public NotebookParams NotebookParams { get; set; }

        [JsonProperty("python_params")]
        public List<string> PythonParams { get; set; }

        [JsonProperty("spark_submit_params")]
        public List<string> SparkSubmitParams { get; set; }

        [JsonProperty("python_named_params")]
        public PythonNamedParams PythonNamedParams { get; set; }

        [JsonProperty("pipeline_params")]
        public PipelineParams PipelineParams { get; set; }

        [JsonProperty("sql_params")]
        public SqlParams SqlParams { get; set; }

        [JsonProperty("dbt_commands")]
        public List<string> DbtCommands { get; set; }
    }

    public class PipelineParams
    {
        [JsonProperty("full_refresh")]
        public bool FullRefresh { get; set; }
    }

    public class PythonNamedParams
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("data")]
        public string Data { get; set; }
    }

    public class Run
    {
        [JsonProperty("job_id")]
        public string JobId { get; set; }

        [JsonProperty("run_id")]
        public long JobRunID { get; set; }

        [JsonProperty("creator_user_name")]
        public string CreatorUserName { get; set; }

        [JsonProperty("original_attempt_run_id")]
        public string OriginalAttemptRunId { get; set; }

        [JsonProperty("state")]
        public State State { get; set; }

        [JsonProperty("schedule")]
        public Schedule Schedule { get; set; }

        [JsonProperty("tasks")]
        public List<JobTask> Tasks { get; set; }

        [JsonProperty("job_clusters")]
        public List<JobCluster> JobClusters { get; set; }

        [JsonProperty("cluster_spec")]
        public ClusterSpec ClusterSpec { get; set; }

        [JsonProperty("cluster_instance")]
        public ClusterInstance ClusterInstance { get; set; }

        [JsonProperty("git_source")]
        public object GitSource { get; set; }

        [JsonProperty("overriding_parameters")]
        public OverridingParameters OverridingParameters { get; set; }

        [JsonProperty("start_time")]
        public long StartTime { get; set; }

        [JsonProperty("setup_duration")]
        public int SetupDuration { get; set; }

        [JsonProperty("execution_duration")]
        public int ExecutionDuration { get; set; }

        [JsonProperty("cleanup_duration")]
        public int CleanupDuration { get; set; }

        [JsonProperty("end_time")]
        public long EndTime { get; set; }

        [JsonProperty("trigger")]
        public string Trigger { get; set; }

        [JsonProperty("run_name")]
        public string RunName { get; set; }

        [JsonProperty("run_page_url")]
        public string RunPageUrl { get; set; }

        [JsonProperty("run_type")]
        public string RunType { get; set; }

        [JsonProperty("attempt_number")]
        public int AttemptNumber { get; set; }

        public string FileGUID
        {
            get
            {
                string firstParameter = this.OverridingParameters?.JarParams?.FirstOrDefault();
                if (firstParameter == null) return null;
                var keyValue = firstParameter.Split('=');
                if (keyValue[0] != "FileGUID") return null;
                return keyValue[1];
            }
        }
    }

    public class Schedule
    {
        [JsonProperty("quartz_cron_expression")]
        public string QuartzCronExpression { get; set; }

        [JsonProperty("timezone_id")]
        public string TimezoneId { get; set; }

        [JsonProperty("pause_status")]
        public string PauseStatus { get; set; }
    }

    public class SparkConf
    {
    }

    public class SparkEnvVars
    {
    }

    public class SparkJarTask
    {
        [JsonProperty("jar_uri")]
        public string JarUri { get; set; }

        [JsonProperty("main_class_name")]
        public string MainClassName { get; set; }

        [JsonProperty("parameters")]
        public List<string> Parameters { get; set; }

        [JsonProperty("run_as_repl")]
        public bool RunAsRepl { get; set; }
    }

    public class SqlParams
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("age")]
        public string Age { get; set; }
    }

    public class State
    {
        [JsonProperty("life_cycle_state")]
        public string LifeCycleState { get; set; }

        [JsonProperty("result_state")]
        public string ResultState { get; set; }

        public ResultState ResultStateEnum
        {
            get
            {
                ResultState outvalue;
                if (string.IsNullOrEmpty(this.ResultState))
                {
                    return Databricks.ResultState.NONE;
                }

                if (Enum.TryParse(this.ResultState, out outvalue))
                {
                    return outvalue;
                }
                else
                {
                    throw new NotSupportedException("ResultState contains a string not supported by the Enum ResultState");
                }
            }
        }

        [JsonProperty("user_cancelled_or_timedout")]
        public bool UserCancelledOrTimedout { get; set; }

        [JsonProperty("state_message")]
        public string StateMessage { get; set; }
    }

    public class JobTask
    {
        [JsonProperty("run_id")]
        public long RunId { get; set; }

        [JsonProperty("task_key")]
        public string TaskKey { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; }

        [JsonProperty("job_cluster_key")]
        public string JobClusterKey { get; set; }

        [JsonProperty("spark_jar_task")]
        public SparkJarTask SparkJarTask { get; set; }

        [JsonProperty("libraries")]
        public List<object> Libraries { get; set; }

        [JsonProperty("state")]
        public State State { get; set; }

        [JsonProperty("run_page_url")]
        public string RunPageUrl { get; set; }

        [JsonProperty("start_time")]
        public object StartTime { get; set; }

        [JsonProperty("setup_duration")]
        public int SetupDuration { get; set; }

        [JsonProperty("execution_duration")]
        public int ExecutionDuration { get; set; }

        [JsonProperty("cleanup_duration")]
        public int CleanupDuration { get; set; }

        [JsonProperty("end_time")]
        public object EndTime { get; set; }

        [JsonProperty("cluster_instance")]
        public ClusterInstance ClusterInstance { get; set; }

        [JsonProperty("attempt_number")]
        public int AttemptNumber { get; set; }

        [JsonProperty("depends_on")]
        public List<object> DependsOn { get; set; }

        [JsonProperty("new_cluster")]
        public NewCluster NewCluster { get; set; }

        [JsonProperty("notebook_task")]
        public NotebookTask NotebookTask { get; set; }

        [JsonProperty("existing_cluster_id")]
        public string ExistingClusterId { get; set; }
    }
}

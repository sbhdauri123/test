using Greenhouse.DAL.Databricks.RunListResponse;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.DAL.Databricks;

public class Response
{
    public Metadata Metadata { get; set; }
    public string Error { get; set; }
    public string ErrorTrace { get; set; }
    public object NotebookOutput { get; set; }
}

public class Metadata
{
    public long JobId { get; set; }
    public long RunId { get; set; }
    public string CreatorUserName { get; set; }
    public long NumberInJob { get; set; }
    public long OriginalAttemptRunId { get; set; }
    public JobState JobState { get; set; }
    public List<JobParameter> JobParameters { get; set; }
    public long StartTime { get; set; }
    public int SetupDuration { get; set; }
    public int ExecutionDuration { get; set; }
    public int CleanupDuration { get; set; }
    public long EndTime { get; set; }
    public string Trigger { get; set; }
    public string RunName { get; set; }
    public string RunPageUrl { get; set; }
    public string RunType { get; set; }
    public long ParentRunId { get; set; }

    [JsonProperty("tasks")]
    public List<JobTask> Tasks { get; set; }
    public string TaskKey { get; set; }
    public string Format { get; set; }
    public Status Status { get; set; }
    public long JobRunId { get; set; }
}

public class JobState
{
    public string LifeCycleState { get; set; }
    public string ResultState { get; set; }
    public string StateMessage { get; set; }
    public bool UserCancelledOrTimedOut { get; set; }
}

public class JobParameter
{
    public string Name { get; set; }
    public string DefaultValue { get; set; }
    public string Value { get; set; }
}

public class JobTask
{
    public long RunId { get; set; }

    public string TaskKey { get; set; }

    public NotebookTask NotebookTask { get; set; }

    public State State { get; set; }

    public string RunPageUrl { get; set; }

    public long StartTime { get; set; }

    public int SetupDuration { get; set; }

    public int ExecutionDuration { get; set; }

    public int CleanupDuration { get; set; }

    public long EndTime { get; set; }

    public int AttemptNumber { get; set; }

    public Status Status { get; set; }
}

public class NotebookTask
{
    public string NotebookPath { get; set; }

    public string Source { get; set; }
}

public class Status
{
    public string State { get; set; }

    public TerminationDetails TerminationDetails { get; set; }
}

public class TerminationDetails
{
    public string Code { get; set; }
    public string Type { get; set; }
    public string Message { get; set; }
}

using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

public class SnapshotReportReponse
{
    [JsonProperty("snapshotId")]
    public string SnapShotId { get; set; }

    [JsonProperty("details")]
    public string Details { get; set; }

    [JsonProperty("jobStatus")]
    public SnapshotJobStatus? JobStatus { get; set; } = null;
}

public enum SnapshotJobStatus
{
    Pending,
    Processing,
    Done,
    Failed,
    Expired
}

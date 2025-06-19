using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

public class SnapshotDownloadRequestBody
{
    [JsonProperty("advertiserId")]
    public int AdvertiserId { get; set; }

    [JsonProperty("snapshotId")]
    public string SnapShotId { get; set; }
}

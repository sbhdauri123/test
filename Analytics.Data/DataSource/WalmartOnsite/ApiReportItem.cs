using Greenhouse.Data.Model.Aggregate;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

public class ApiReportItem
{
    private const int SNAPSHOTID_EXPIRATION_IN_HOURS = 24;

    public long QueueID { get; set; }
    public string ReportName { get; set; }
    public Guid FileGuid { get; set; }
    public DateTime ReportDate { get; set; }
    public int AdvertiserID { get; set; }
    public string SnapShotEntity { get; set; }
    public IEnumerable<APIReportField> ReportFields { get; set; }
    public string SnapShotID { get; set; }
    public DateTime SnapShotIDGenerated { get; set; }
    public string DownloadURI { get; set; }
    public ReportType ReportType { get; set; }
    public string FileExtension { get; set; }
    public string AttributionWindow { get; set; }
    public string Version { get; set; }
    public EntityStatus EntityStatus { get; set; }
    public List<string> EntityTypes { get; set; }


    public bool SnapshotHasBeenGenerated { get; set; }
    public bool IsReadyForDownload { get; set; }
    public bool IsDownloaded { get; set; }
    public bool IsFailed { get; set; }

    public bool ShouldGenerateSnapshot => IsFailed || !SnapshotHasBeenGenerated || ((DateTime.UtcNow - SnapShotIDGenerated).TotalHours >= SNAPSHOTID_EXPIRATION_IN_HOURS);
}

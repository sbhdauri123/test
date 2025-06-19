using Greenhouse.Data.Model.Core;
using System;

namespace Greenhouse.Data.DataSource.Twitter;

[Serializable]
public class ApiReportItem
{
    public string ReportName { get; set; }
    public long QueueID { get; set; }
    public Guid FileGuid { get; set; }
    public string AccountID { get; set; }
    public string Status { get; set; }
    public string ReportID { get; set; }
    public bool IsReady { get; set; }
    public bool IsDownloaded { get; set; }
    public string ReportURL { get; set; }
    public string FileExtension { get; set; }
    public string Placement { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public bool IsStaticDimension { get; set; }
    public bool StaticDimensionDownloaded { get; set; }
    public Model.Aggregate.APIReport<ReportSettings> ApiReport { get; set; }
    public FileCollectionItem FileCollectionItem { get; set; }
    public DateTime LastWriteTimeUtc { get; set; }
    public string DMACountryID { get; set; }
    public bool HasNoActiveEntities { get; set; }
}
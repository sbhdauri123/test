using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.AmazonAdsApi;

[Serializable]
public class APIReportItem
{
    public APIReportItem() { }


    public string ReportId { get; set; }
    public string ReportName { get; set; }
    public long QueueID { get; set; }
    public Guid FileGuid { get; set; }
    public string ProfileID { get; set; }
    public string Status { get; set; }
    public string ReportToken { get; set; }
    public bool IsReady { get; set; }
    public bool IsDownloaded { get; set; }
    public Uri ReportURL { get; set; }
    public string FileName { get; set; }
    public string FileExtension { get; set; }
    public string FileSize { get; set; }
    public Model.Core.FileCollectionItem FileItem { get; set; }
    public int APIReportID { get; set; }
    public ReportSettings ReportSettings { get; set; }
    public DateTime? FileDate { get; set; }
    public IEnumerable<Model.Core.FileCollectionItem> FileCollection { get; set; }
    public DateTime? TaskRunDate { get; set; }
    public string ReportStartDate { get; set; }
    public string ReportEndDate { get; set; }

    public List<string> AdvertiserIds { get; set; }

    public int? BatchId { get; set; }

}

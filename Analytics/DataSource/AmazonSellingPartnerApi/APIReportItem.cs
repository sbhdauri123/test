using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi;

[Serializable]
public class APIReportItem
{
    public APIReportItem() { }

    public string ReportId { get; set; }
    public string ReportName { get; set; }
    public long QueueID { get; set; }
    public Guid FileGuid { get; set; }
    public string MarketplaceId { get; set; }
    public string Status { get; set; }
    public bool IsReady { get; set; }
    public bool IsDownloaded { get; set; }
    public string FileExtension { get; set; }
    public Model.Core.FileCollectionItem FileItem { get; set; }
    public int APIReportID { get; set; }
    public DateTime? FileDate { get; set; }
    public DateTime? TaskRunDate { get; set; }
    public string ReportStartDate { get; set; }
    public string ReportEndDate { get; set; }
    public string ReportDocumentId { get; set; }

    public List<string> MarketplaceIds { get; set; }

}

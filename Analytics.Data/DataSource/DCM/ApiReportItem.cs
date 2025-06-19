using Greenhouse.Data.Model.Core;
using System;

namespace Greenhouse.Data.DataSource.DCM
{
    [Serializable]
    public class ApiReportItem
    {
        public ApiReportItem() { }
        public string ReportName { get; set; }

        public int? PageNumber { get; set; }
        public long QueueID { get; set; }

        public Guid FileGuid { get; set; }
        public string ProfileID { get; set; }
        public ReportStatus Status { get; set; }
        public long ReportID { get; set; }
        public bool IsSubmitted { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }
        public string ReportURL { get; set; }
        public string FileID { get; set; }
        public string FileExtension { get; set; }
        public DateTime TimeSubmitted { get; set; }
        public FileCollectionItem FileItem { get; set; }

        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }

        public bool IsPlaceholder => ReportID == 0;
    }
}
using Greenhouse.Data.Model.Aggregate;
using System;

namespace Greenhouse.Data.DataSource.LinkedIn
{
    public class ApiReportItem
    {
        public string ReportName { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string ReportType { get; set; }
        public APIReport<ReportSettings> APIReport { get; set; }
        public DateTime? FileDate { get; set; }
        public string AccountID { get; set; }
        public Model.Core.FileCollectionItem FileCollection { get; set; }
        public string FileName { get; set; }
        public bool IsDownloaded { get; set; }
        public DateTime? DeliveryFileDate { get; set; }

        public bool IsReady { get; set; }
        public string ReportID { get; set; }
        public long FileSize { get; set; }
    }
}

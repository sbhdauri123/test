using System;

namespace Greenhouse.Data.DataSource.GoogleAds.Aggregate
{
    [Serializable]
    public class ApiReportItem
    {
        public ApiReportItem() { }
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

        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }
}

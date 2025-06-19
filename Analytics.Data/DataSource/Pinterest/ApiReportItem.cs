using Greenhouse.Data.Model.Aggregate;
using System;

namespace Greenhouse.Data.DataSource.Pinterest
{
    [Serializable]
    public class ApiReportItem
    {
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string ProfileID { get; set; }
        public string Status { get; set; }
        public string ReportToken { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }
        public string ReportURL { get; set; }
        public string FileName { get; set; }
        public string FileExtension { get; set; }
        public DateTime? FileDate { get; set; }
        public Model.Core.FileCollectionItem FileCollection { get; set; }
        public DateTime? TaskRunDate { get; set; }
        public APIReport<ReportSettings> Report { get; set; }
    }
}
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.TikTok
{
    [Serializable]
    public class APIReportItem
    {
        public APIReportItem() { }
        public string ReportName { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string ProfileID { get; set; }
        public bool IsDownloaded { get; set; }
        public bool HasFailedToDownload { get; set; }
        public string FileExtension { get; set; }
        public Model.Core.FileCollectionItem FileItem { get; set; }
        public bool IsDimension { get; set; }
        public int APIReportID { get; set; }
        public ReportSettings ReportSettings { get; set; }
        public DateTime? FileDate { get; set; }
        public IEnumerable<Model.Core.FileCollectionItem> FileCollection { get; set; }
        public DateTime? TaskRunDate { get; set; }

        public string Status { get; set; }
        public string ReportToken { get; set; }
        public string ReportURL { get; set; }
        public string FileName { get; set; }
        public bool DimensionDownloaded { get; set; }
    }
}
using System;

namespace Greenhouse.Data.DataSource.Amazon
{
    [Serializable]
    public class ReportItem
    {
        public ReportItem() { }
        public string ReportName { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string FileName { get; set; }
        public bool IsDownloaded { get; set; }
        public string FileExtension { get; set; }
        public long FileSize { get; set; }
    }
}
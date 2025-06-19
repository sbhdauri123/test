using Greenhouse.Data.Model.Aggregate;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Reddit
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
        public List<Model.Core.FileCollectionItem> FileCollectionList { get; set; } = new();
        public string FileName { get; set; }
        public bool IsDownloaded { get; set; }
        public int CurrentPageIndex { get; set; }
    }
}

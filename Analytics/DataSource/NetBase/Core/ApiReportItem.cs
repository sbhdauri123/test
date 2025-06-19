using System;

namespace Greenhouse.Data.DataSource.NetBase.Core
{
    [Serializable]
    public class ApiReportItem
    {
        public ApiReportItem() { }
        public string ReportName { get; set; }
        public long ReportSize { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string TopicID { get; set; }
        public string ThemeID { get; set; }
        public string ReportType { get; set; }
        public string ReportMetric { get; set; }
    }
}
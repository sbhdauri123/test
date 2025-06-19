using System;

namespace Greenhouse.Data.DataSource.DBM.API
{
    [Serializable]
    public class ApiReportItem : DataSource.DCM.ApiReportItem
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        public long QueryID { get; set; }

        public DateTime FileDate { get; set; }
        public string FilePath { get; set; }
        public long FileSize { get; set; }
        public Model.Core.FileCollectionItem FileCollection { get; set; }
        public DateTime? DeliveryFileDate { get; set; }
        public string ReportType { get; set; }
    }
}
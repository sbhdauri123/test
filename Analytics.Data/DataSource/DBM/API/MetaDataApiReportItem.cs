using Greenhouse.Data.Model.Aggregate;
using System;

namespace Greenhouse.Data.DataSource.DBM.API
{
    public class MetadataApiReportItem : DataSource.DCM.ApiReportItem
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        public long QueryID { get; set; }

        public DateTime FileDate { get; set; }
        public string FilePath { get; set; }
        public string EndPath { get; set; }
        public long FileSize { get; set; }
        public Model.Core.FileCollectionItem FileCollection { get; set; }
        public DateTime? DeliveryFileDate { get; set; }
        public string ReportType { get; set; }
        public APIReport<ReportSettings> APIReport { get; set; }
    }
}
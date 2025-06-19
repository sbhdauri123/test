using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class EventLevelDataStatus : BasePOCO
    {
        public EventLevelDataStatus() { }

        [Key]
        public int IntegrationID { get; set; }
        public int SourceID { get; set; }
        [Key]
        public string ValidationFileName { get; set; }
        public DateTime ExpectedDate { get; set; }
        public DateTime MaxFileDate { get; set; }
        public int FileCountExpected { get; set; }
        public int TotalFileCountDelivered { get; set; }
        public bool FileTypeStatus { get; set; }
        public string ClientSLA { get; set; }
        public string VendorSLA { get; set; }
        public string Cadence { get; set; }
        public DateTime PMMetastoreMaxPartitionDate { get; set; }
        public int PMMetastoreMaxPartitionHour { get; set; }
        public int PendingPartitions { get; set; }
        public int DataIngestionStatusID { get; set; }
        public int DataProcessingStatusID { get; set; }
    }
}

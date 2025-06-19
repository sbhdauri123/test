using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class HivePartition
    {
        public long PartitionID { get; set; }
        public string PartitionPath { get; set; }
        public int AdvertiserMappingID { get; set; }
        public string HiveTableName { get; set; }
        public string DataSourceName { get; set; }
        public int InstanceID { get; set; }
        public string InstanceName { get; set; }
        public string EMRClusterID { get; set; }
        public string EntityName { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Hour { get; set; }

        public string SourceTypeName { get; set; }

        public string FileType { get; set; }
        public string AdvertiserID { get; set; }
        public System.Guid FileGUID { get; set; }

        public bool IsPMInstance { get; set; }
    }
}

using System;

namespace Greenhouse.Data.Model.DataStatus
{
    public class AggDateResult
    {
        public string AdvertiserID { get; set; }
        public string AdvertiserName { get; set; }
        public DateTime MaxDataDate { get; set; }
        public DateTime MinDataDate { get; set; }
        public DateTime LastUpdated { get; set; }
        public DateTime CreatedDate { get; set; }
        public string FileGUID { get; set; }
        public string MasterAdvertiserID { get; set; }
        public string MasterAdvertiserName { get; set; }
        public string MasterBusinessUnitID { get; set; }
        public string MasterBusinessUnitName { get; set; }
        public int SourceID { get; set; }
        public int DataSourceID { get; set; }
    }
}

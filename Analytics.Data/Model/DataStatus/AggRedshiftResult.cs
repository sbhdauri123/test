using System;

namespace Greenhouse.Data.Model.DataStatus
{
    public class AggRedshiftResult
    {
        public string MasterBusinessUnitID { get; set; }
        public string MasterBusinessUnitName { get; set; }
        public string MasterAdvertiserID { get; set; }
        public string MasterAdvertiserName { get; set; }
        public string MasterCountryID { get; set; }
        public string MasterCountryName { get; set; }
        public string MasterAgencyID { get; set; }
        public string MasterAgencyName { get; set; }
        public string ParentID { get; set; }
        public string ParentName { get; set; }
        public string AdvertiserID { get; set; }
        public string AdvertiserName { get; set; }
        public DateTime? DataMaxDate { get; set; }
        public string FileGUID { get; set; }
        public int SourceID { get; set; }
        public bool IsPlaceholder { get; set; }
    }
}

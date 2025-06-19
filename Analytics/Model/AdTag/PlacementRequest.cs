using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class PlacementRequest : BasePOCO
    {
        [Key]
        public int PlacementRequestID { get; set; }
        [Dapper.NotMapped]
        public int AdVendorID { get; set; }
        [Dapper.NotMapped]
        public string AdVendorName { get; set; }
        public long AccountID { get; set; }
        public long AdvertiserID { get; set; }
        [Dapper.NotMapped]
        public string AdvertiserName { get; set; }
        public long ProfileID { get; set; }
        public string Status { get; set; }
        public int StatusSortOrder { get; set; }
        public string PlacementsRequested { get; set; }
        public string RequestedBy { get; set; }
    }
}

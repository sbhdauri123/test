using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class AdVendor : BasePOCO
    {
        [Key]
        public int AdVendorID { get; set; }
        public string AdVendorName { get; set; }
        public int AdVendorTypeID { get; set; }
        public int DateValueID { get; set; }
    }
}

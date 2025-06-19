using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class Advertiser : BasePOCO
    {
        [Key]
        public long AdvertiserID { get; set; }
        public string AdvertiserName { get; set; }
        public int AdVendorID { get; set; }
        public string AdVendorName { get; set; }
        public long ProfileID { get; set; }
        public long AccountID { get; set; }
        public string DDLKey
        {
            get
            {
                return string.Format("{0}~!~{1}", AdvertiserID.ToString(), AdVendorName);
            }
        }
    }
}

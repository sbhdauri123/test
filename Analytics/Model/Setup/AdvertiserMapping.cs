using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class AdvertiserMapping : BasePOCO
    {
        public AdvertiserMapping()
        {
        }

        [Key]
        public int AdvertiserMappingID { get; set; }
        public string AdvertiserID { get; set; }
        public int DataSourceID { get; set; }
        public string DataSourceName { get; set; }
        public string AdvertiserName { get; set; }
        [Dapper.NotMapped]
        public string AdvertiserNameDisplay
        {
            get
            {
                return string.Format("{0} ({1})", AdvertiserName, AdvertiserID);
            }
        }
        public int? MasterClientID { get; set; }
        public int? IntegrationID { get; set; }
        public int? CountryID { get; set; }
        public string TimeZone { get; set; }
        public bool IsActive { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SA360CustomField : BasePOCO
    {
        [Key]
        public string AgencyID { get; set; }
        public string AgencyName { get; set; }
        public string AdvertiserID { get; set; }
        public string AdvertiserName { get; set; }
        public string SavedColumnName { get; set; }
        public bool IsActive { get; set; }
        new public DateTime CreatedDate { get; set; }
        new public DateTime LastUpdated { get; set; }
    }
}
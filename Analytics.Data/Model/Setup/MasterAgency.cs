using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class MasterAgency : BasePOCO
    {
        [Key]
        public int MasterAgencyID { get; set; }
        public string MasterAgencyCode { get; set; }
        public string MasterAgencyDescription { get; set; }
        public bool IsActive { get; set; }
    }
}

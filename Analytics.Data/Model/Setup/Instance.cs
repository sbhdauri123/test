using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Instance : BasePOCO
    {
        [Key]
        public int InstanceID { get; set; }
        public string InstanceName { get; set; }
        public string EMRClusterId { get; set; }
        public string UserID { get; set; }
        public int CountryID { get; set; }
        public int MasterAgencyID { get; set; }
        public string AuthToken { get; set; }
        public bool isActive { get; set; }
        public bool isPM { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Security : BasePOCO
    {
        [Key]
        public int MasterClientID { get; set; }
        [Key]
        public int MasterAgencyID { get; set; }
        [Key]
        public string Username { get; set; }
    }
}

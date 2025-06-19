using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class MasterClient : BasePOCO
    {
        [Key]
        public int MasterClientID { get; set; }
        public string MasterClientCode { get; set; }
        public string MasterClientDescription { get; set; }
        public bool IsActive { get; set; }
    }
}

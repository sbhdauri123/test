using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class Status : BasePOCO
    {
        [Key]
        public int StatusID { get; set; }
        public string StatusValue { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class ServerType : BasePOCO
    {
        [Key]
        public int ServerTypeID { get; set; }
        public string ServerTypeName { get; set; }
    }
}

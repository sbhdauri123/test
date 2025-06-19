using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class ETLType : BasePOCO
    {
        [Key]
        public int ETLTypeID { get; set; }
        public string ETLTypeName { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class EntityETLMap : BasePOCO
    {
        [Key]
        public int APIEntityID { get; set; }
        public int ETLScriptID { get; set; }
    }
}

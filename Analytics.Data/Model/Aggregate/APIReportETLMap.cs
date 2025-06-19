using Dapper;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class APIReportETLMap : BasePOCO
    {
        [Key]
        public int APIReportETLMapID { get; set; }
        public int ETLScriptID { get; set; }
        public int APIReportID { get; set; }
    }
}

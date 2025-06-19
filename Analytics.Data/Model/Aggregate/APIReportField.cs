using Dapper;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class APIReportField : BasePOCO
    {
        [Key]
        public int APIReportFieldID { get; set; }
        public string APIReportFieldName { get; set; }
        public int APIReportID { get; set; }
        public int SortOrder { get; set; }
        public bool IsActive { get; set; }

        public bool IsDimensionField { get; set; }
    }
}
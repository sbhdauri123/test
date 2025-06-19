using Dapper;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class APIEntity : BasePOCO
    {
        [Key] public int APIEntityID { get; set; }
        public string APIEntityCode { get; set; }
        public string APIEntityName { get; set; }
        public int SourceID { get; set; }
        public int IntegrationID { get; set; }
        public DateTime? StartDate { get; set; }
        public bool IsActive { get; set; }
        public string TimeZone { get; set; }
        public int? EntityPriorityOrder { get; set; }
        public bool? BackfillPriority { get; set; }
    }
}
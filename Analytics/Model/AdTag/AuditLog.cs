using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class AuditLog
    {
        [Key]
        public Int32 AuditLogId { get; set; }
        public string AppComponent { get; set; }
        public string Action { get; set; }
        public string OriginalValue { get; set; }
        public string ModifiedValue { get; set; }
        public DateTime DateModified { get; set; }
        public string ModifiedBy { get; set; }
    }
}

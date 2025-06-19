using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    public class AuditLog
    {
        [Key]
        public long AuditLogId { get; set; }

        public string AppComponent { get; set; }

        public string Action { get; set; }

        public string ModifiedBy { get; set; }

        public string AdditionalDetails { get; set; }

        private DateTime createdDate;
        public DateTime CreatedDate
        {
            get
            {
                return (this.createdDate == default(DateTime))
                       ? this.createdDate = DateTime.UtcNow
                       : this.createdDate;
            }
            set
            {
                this.createdDate = value;
            }
        }
    }
}

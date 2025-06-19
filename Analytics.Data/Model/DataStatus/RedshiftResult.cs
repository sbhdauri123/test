using System;

namespace Greenhouse.Data.Model.DataStatus
{
    public class RedshiftResult
    {
        public string APIEntityCode { get; set; }
        public string EntityId { get; set; }
        public string EntityName { get; set; }
        public DateTime? DataMaxDate { get; set; }
        public string FileGUID { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DatabricksFailedJobs : BasePOCO
    {
        [Key]
        public long QueueID { get; set; }
        public long RunID { get; set; }
        public string Status { get; set; }
        public long DatabricksJobID { get; set; }
        public string DatabricksJobParameters { get; set; }
        public DateTime StatusUpdatedDate { get; set; }
    }
}

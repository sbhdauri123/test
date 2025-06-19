using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DatabricksJobLog : BasePOCO
    {
        [Key]
        public long ID { get; set; }
        public long QueueID { get; set; }
        public long RunID { get; set; }
        public string Status { get; set; }
        public long DatabricksJobID { get; set; }
        public string DatabricksJobParameters { get; set; }
    }
}

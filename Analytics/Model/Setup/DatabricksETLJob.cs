using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DatabricksETLJob : BasePOCO
    {
        [Key]
        public int ID { get; set; }
        public string JobName { get; set; }
        public int DataSourceID { get; set; }
        public string DatabricksJobID { get; set; }
        public string DatabricksTableName { get; set; }
        public int SourceID { get; set; }
    }
}

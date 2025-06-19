using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DataSource : BasePOCO
    {
        [Key]
        public int DataSourceID { get; set; }
        public string DataSourceName { get; set; }
        public int JobCategoryID { get; set; }

        public int? SourceTypeID { get; set; }
    }
}

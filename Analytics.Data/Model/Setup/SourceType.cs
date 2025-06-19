using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class SourceType : BasePOCO
    {
        [Key]
        public int SourceTypeID { get; set; }
        public string SourceTypeName { get; set; }

        public int? SourceID { get; set; }

        public string DataSourceName { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DataStatusIntegration : BasePOCO
    {
        public DataStatusIntegration() { }

        [Key]
        public int IntegrationID { get; set; }
        public string IntegrationName { get; set; }
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public DateTime MaxFileDate { get; set; }
        public string Status { get; set; }
        public int ETLTypeID { get; set; }
    }
}

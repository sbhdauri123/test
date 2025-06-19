using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class JobStatusSourceFile : BasePOCO
    {
        [Key]
        public int IntegrationID { get; set; }
        public string IntegrationName { get; set; }
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public string FileType { get; set; }
        public DateTime? FileDate { get; set; }
        public string Status { get; set; }
        public string Message { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DataStatusSource : BasePOCO
    {
        public DataStatusSource() { }
        [Key]
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public DateTime MaxFileDate { get; set; }
        public string Status { get; set; }
        public int ETLTypeId { get; set; }
    }
}

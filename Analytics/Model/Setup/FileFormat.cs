using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class FileFormat : BasePOCO
    {
        [Key]
        public int FileFormatID { get; set; }
        public string FileFormatName { get; set; }
    }
}

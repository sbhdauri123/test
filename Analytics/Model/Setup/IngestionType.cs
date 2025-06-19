using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class IngestionType : BasePOCO
    {
        [Key]
        public int IngestionTypeID { get; set; }
        public string IngestionTypeName { get; set; }
    }
}

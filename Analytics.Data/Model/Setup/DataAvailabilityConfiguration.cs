using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class DataAvailabilityConfiguration : BasePOCO
    {
        [Key]
        public int ID { get; set; }
        public int SourceID { get; set; }

        //This column is currently not used, only daily cadence are supported
        public string Cadence { get; set; }
        [Editable(true)]
        public string Comments { get; set; }
        public bool IsActive { get; set; }
        public string SQLQuery { get; set; }
        public string ClientSLA { get; set; }
        public string VendorSLA { get; set; }
        public string RedshiftSchema { get; set; }
        public string RedshiftTable { get; set; }
        public int DataDateOffset { get; set; }
        public int AlertOffset { get; set; }
    }
}

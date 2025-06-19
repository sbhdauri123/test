using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class APIAdServerRequest : BasePOCO
    {
        [Key]
        public int APIAdServerRequestID { get; set; }
        public string AdvertiserName { get; set; }
        public string UserName { get; set; }
        public int ProfileID { get; set; }
        public string TagVersion { get; set; }
        public string PASDetail { get; set; }
        public char PairDelimiter { get; set; }
        public char KeyValueDelimiter { get; set; }
        public bool IsAPIAdServer { get; set; }
        public bool IsActive { get; set; }
        public bool IsOutputToReport { get; set; }
        public bool WriteToReportStatus { get; set; }
        public string AccountID { get; set; }
        public string AdvertiserID { get; set; }
        public long InitialPlacementID { get; set; }
        public int AdVendorID { get; set; }
        public DateTime LastImportDate { get; set; }
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag.APIAdServer
{
    public class APIAdServerRequestMapping : BasePOCO
    {
        [Key]
        public int APIAdServerRequestID { get; set; }
        public string ProfileId { get; set; }
        public string AccountId { get; set; }
        public string UserName { get; set; }
        public string AdvertiserId { get; set; }
        public string AdvertiserName { get; set; }
        public bool IsOutputToReport { get; set; }
        public bool WriteToReportStatus { get; set; }
        public DateTime? LastImportDate { get; set; }
    }
}

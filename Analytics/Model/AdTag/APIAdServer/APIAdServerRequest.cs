using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag.APIAdServer

{
    public class APIAdServerRequest : BasePOCO
    {
        [Key]
        public int APIAdServerRequestID { get; set; }
        public bool WriteToReportStatus { get; set; }
        public DateTime LastImportDate { get; set; }
        new public DateTime LastUpdated
        {
            get
            {
                return (this.lastUpdated == default(DateTime))
                   ? this.lastUpdated = DateTime.Now
                   : this.lastUpdated;
            }

            set { this.lastUpdated = value; }
        }
        private DateTime lastUpdated;
    }
}

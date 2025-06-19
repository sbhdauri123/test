using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag.APIAdServer
{
    public class AdvertiserJobDetail
    {
        [Key]
        [Required]
        public long AdvertiserID { get; set; }
        public long InitialPlacementID { get; set; }
        public long LastProcessedPlacementID { get; set; }
        public DateTime LastUpdated
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
        public DateTime CreatedDate
        {
            get
            {
                return (this.createdDate == default(DateTime))
                    ? this.createdDate = DateTime.Now
                    : this.createdDate;
            }

            set { this.createdDate = value; }
        }
        private DateTime createdDate;
    }
}

using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class MappedAdvertiser
    {
        [Key]
        public int AdvertiserMappingID { get; set; }
        private string AdvertiserID { get; set; }
        private string AdvertiserName { get; set; }
        [Dapper.NotMapped]
        public string AdvertiserNameDisplay
        {
            get
            {
                return string.Format("{0} ({1})", AdvertiserName, AdvertiserID);
            }
        }
        public bool IsAggregate { get; set; }
    }
}
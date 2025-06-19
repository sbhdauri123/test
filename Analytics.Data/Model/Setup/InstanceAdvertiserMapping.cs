using Dapper;
using System;

namespace Greenhouse.Data.Model.Setup
{
    [Serializable]
    public class InstanceAdvertiserMapping
    {
        public InstanceAdvertiserMapping()
        {
        }

        [Key]
        public int AdvertiserMappingID { get; set; }
        public string AdvertiserID { get; set; }
        public int InstanceID { get; set; }
        public string AdvertiserName { get; set; }
        [Dapper.NotMapped]
        public string AdvertiserNameDisplay
        {
            get
            {
                return string.Format("{0} ({1})", AdvertiserName, AdvertiserID);
            }
        }
        public string Flag { get; set; }
    }
}
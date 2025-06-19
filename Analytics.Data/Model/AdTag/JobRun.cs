using Dapper;
using System;

namespace Greenhouse.Data.Model.AdTag
{
    [Serializable]
    public class JobRun : BasePOCO
    {
        [Key]
        public int JobRunId { get; set; }

        [Dapper.NotMapped]
        public string Advertiser
        {
            get
            {
                return string.Format("{0} [{1}]", AdvertiserName, AdvertiserId.ToString());
            }
        }
        public long AdvertiserId { get; set; }
        public string AdvertiserName { get; set; }
        public long LastPlacementId { get; set; }

        [Dapper.NotMapped]
        public string DisplayLastPlacementId
        {
            get
            {
                return (LastPlacementId == 0 ? "" : LastPlacementId.ToString());
            }
        }
        public long StartPlacementId { get; set; }

        [Dapper.NotMapped]
        public string ExecutionTime
        {
            get
            {
                var executionTime = Status == "Running" ? DateTime.UtcNow.Subtract(CreatedDate) : LastUpdated.Subtract(CreatedDate);
                return executionTime.ToString(@"d\:h\:mm\:ss\.f");
            }
        }
        public string Status { get; set; }
        public bool HasuValueError { get; set; }
        public string Message { get; set; }
        public int PlacementsModified { get; set; }

        [Dapper.NotMapped]
        public string DisplayHasuValueError
        {
            get
            {
                return (HasuValueError ? "Yes" : "No");
            }
        }
    }
}

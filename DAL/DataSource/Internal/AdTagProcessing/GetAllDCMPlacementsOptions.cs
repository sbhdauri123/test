using Greenhouse.Data.DataSource.Twitter;
using System;

namespace Greenhouse.DAL.DataSource.Internal.AdTagProcessing
{
    public record GetAllDCMPlacementsOptions
    {
        public string ProfileId { get; init; }
        public string AdvertiserID { get; init; }
        public DateTime StartDate { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ProfileId))
            {
                throw new ArgumentException("ProfileId is required.", nameof(ProfileId));
            }

            if (string.IsNullOrWhiteSpace(AdvertiserID))
            {
                throw new ArgumentException("AdvertiserID is required.", nameof(AdvertiserID));
            }

            if (StartDate == default)
            {
                throw new ArgumentNullException(nameof(ReportParameters), "StartDate is required");
            }
        }
    }
}

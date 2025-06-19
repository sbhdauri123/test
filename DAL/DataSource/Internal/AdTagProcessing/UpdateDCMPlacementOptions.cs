using Greenhouse.Data.Model.AdTag.APIAdServer;
using System;

namespace Greenhouse.DAL.DataSource.Internal.AdTagProcessing
{
    public record class UpdateDCMPlacementOptions
    {
        public string ProfileId { get; init; }
        public Placement Placement { get; init; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ProfileId))
            {
                throw new ArgumentException("ProfileId is required.", nameof(ProfileId));
            }

            if (Placement == default)
            {
                throw new ArgumentNullException(nameof(Placement), "Placement is required");
            }
        }
    }
}

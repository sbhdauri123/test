using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;

namespace Greenhouse.UI.Services.Setup
{
    public class AdvertiserAdminHub : BaseHub<AdvertiserMapping>
    {
        private readonly AdvertiserMappingRepository _repo;
        public AdvertiserAdminHub() : base()
        {
            _repo = new AdvertiserMappingRepository();
        }

        public IEnumerable<AdvertiserMapping> ReadAll(string guid)
        {
            string proc = "GetAvailableAdvertisersByIDs";
            var securityAdvertiserMaps = _repo.GetAll<AdvertiserMapping>(proc, new KeyValuePair<string, string>("DataSourceID", guid));
            return securityAdvertiserMaps;
        }

        public override AdvertiserMapping Update(AdvertiserMapping item)
        {
            SetLastUpdated(item);
            _repo.Update(item);

            return item;
        }
    }
}
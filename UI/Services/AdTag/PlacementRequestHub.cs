using Greenhouse.Common;
using Greenhouse.Data.Model.AdTag;
using Greenhouse.Data.Repositories;

namespace Greenhouse.UI.Services.AdTag
{
    public class PlacementRequestHub : BaseHub<PlacementRequest>
    {
        public PlacementRequestHub(Greenhouse.Data.Services.AdTagService service) : base(service)
        {
        }

        private AdTagAdvertiserRepository adTagAdvertiserRepository;

        public IEnumerable<PlacementRequest> ReadAll(string advertiserID)
        {
            adTagAdvertiserRepository = new AdTagAdvertiserRepository();

            IEnumerable<PlacementRequest> placementRequests = adTagAdvertiserRepository.GetAll<PlacementRequest>("GetAllPlacementRequestsByAdvertiserID", new KeyValuePair<string, string>("AdvertiserID", advertiserID));

            return placementRequests;
        }

        public PlacementRequest ProcessPlacementRequest(PlacementRequest placementRequest)
        {
            adTagAdvertiserRepository = new AdTagAdvertiserRepository();
            AdTagPlacementRequestRepository placementRequestRepository = new AdTagPlacementRequestRepository();

            Advertiser advertiser = adTagAdvertiserRepository.GetAdvertiserDetails(placementRequest.AdvertiserID);

            placementRequest.AdvertiserName = advertiser.AdvertiserName;
            placementRequest.AccountID = advertiser.AccountID;
            placementRequest.ProfileID = advertiser.ProfileID;
            placementRequest.Status = Constants.AdTagPlacementRequestStatus.Pending.ToString();
            placementRequest.RequestedBy = Context.User?.Identity?.Name ?? "Unknown";

            var placementRequestID = placementRequestRepository.Add(placementRequest);

            placementRequest.PlacementRequestID = Convert.ToInt32(placementRequestID);

            return placementRequest;
        }
    }
}
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.Model.AdTag.APIAdServer
{
    public class PlacementGroup
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("subaccountId")]
        public string SubAccountID { get; set; }

        [JsonProperty("accountId")]
        public string AccountID { get; set; }

        [JsonProperty("advertiserId")]
        public string AdvertiserID { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("siteId")]
        public string SiteID { get; set; }

        [JsonProperty("childPlacementIds")]
        public List<string> PlacementIDs { get; set; }

        [JsonProperty("pricingSchedule")]
        public PricingSchedule PricingSchedule { get; set; }
    }

    public class PlacementGroupResponse
    {
        [JsonProperty("kind")]
        public string Kind { get; set; }
        [JsonProperty("nextPageToken")]
        public string NextPageToken { get; set; }
        [JsonProperty("placementGroups")]
        public List<PlacementGroup> PlacementGroups { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class AdUnit
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("adunit_id")]
        public string AdUnitId { get; set; }
        [JsonProperty("campaign_id")]
        public string CampaignId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("placement_ids")]
        public List<string> PlacementIdList { get; set; }
    }
}

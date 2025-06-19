using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.RUNDSP.RUN1
{
    public class Placement
    {
        [JsonProperty("_id")]
        public string Id { get; set; }
        [JsonProperty("campaign_id")]
        public string CampaignId { get; set; }
        [JsonProperty("delivery_type")]
        public string DeliveryType { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("placement_id")]
        public string PlacementId { get; set; }
        [JsonProperty("placement_objective")]
        public PlacementObjective PlacementObjective { get; set; }
    }

    public class PlacementObjective
    {
        [JsonProperty("end_at")]
        public string EndAt { get; set; }
        [JsonProperty("start_at")]
        public string StartAt { get; set; }
    }
}

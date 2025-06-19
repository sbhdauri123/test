using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Twitter;

public class AdsActiveEntity
{
    [JsonProperty("activity_start_time")]
    public string ActivityStartTime { get; set; }
    [JsonProperty("activity_end_time")]
    public string ActivityEndTime { get; set; }
    [JsonProperty("entity_id")]
    public string EntityId { get; set; }
    [JsonProperty("placements")]
    public List<string> Placements { get; set; }
}
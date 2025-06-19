using Newtonsoft.Json;

using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.WalmartOnsite;

public class EntitySnapshotRequestBody
{
    [JsonProperty("advertiserId")]
    public int AdvertiserId { get; set; }

    [JsonProperty("entityStatus")]
    public string EntityStatus { get; set; }

    [JsonProperty("entityTypes")]
    public List<string> EntityTypes { get; set; }

    [JsonProperty("format")]
    public string Format { get; set; }
}

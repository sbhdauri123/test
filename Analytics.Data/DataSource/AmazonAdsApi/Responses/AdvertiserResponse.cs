using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.AmazonAdsApi.Responses;

public class AdvertiserResponse
{
    [JsonProperty("totalResults")]
    public int TotalResults { get; set; }

    [JsonProperty("response")]
    public List<Response> Response { get; set; }
}

public class Response
{
    [JsonProperty("advertiserId")]
    public string AdvertiserId { get; set; }

    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("currency")]
    public string Currency { get; set; }

    [JsonProperty("url")]
    public string Url { get; set; }

    [JsonProperty("country")]
    public string Country { get; set; }

    [JsonProperty("timezone")]
    public string Timezone { get; set; }
}

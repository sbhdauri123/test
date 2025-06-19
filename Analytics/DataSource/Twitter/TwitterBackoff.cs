using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Twitter;

public class TwitterBackoff
{
    [JsonProperty("counter")]
    public int Counter { get; set; }

    [JsonProperty("maxRetry")]
    public int MaxRetry { get; set; }
}
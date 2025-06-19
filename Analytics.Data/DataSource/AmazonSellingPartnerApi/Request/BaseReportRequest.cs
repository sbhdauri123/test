using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Request;

[Serializable]
public record BaseReportRequest
{
    [JsonProperty("dataStartTime")]
    public string DataStartTime { get; set; }

    [JsonProperty("dataEndTime")]
    public string DataEndTime { get; set; }
}

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;

namespace Greenhouse.Data.DataSource.Twitter;

[Serializable]
public class ActiveEntities
{
    [JsonProperty("request")]
    public ReportRequest Request { get; set; }

    [JsonProperty("next_cursor")]
    public string NextCursor { get; set; }

    [JsonProperty("data")]
    public List<AdsActiveEntity> Data { get; set; }

    public Dictionary<string, string> Header { get; set; }
    public HttpStatusCode ResponseCode { get; set; }
}
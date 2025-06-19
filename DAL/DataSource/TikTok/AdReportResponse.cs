using Newtonsoft.Json;
using System.Collections.Generic;


namespace Greenhouse.DAL.DataSource.TikTok;

public class Ad
{
    [JsonProperty("ad_id")]
    public string AdId { get; set; }
}

public class AdReportList
{
    [JsonProperty("list")]
    public List<Ad> AdIds { get; set; }
}

public class AdReportResponse : BaseResponse
{
    [JsonProperty("data")]
    public AdReportList AdReportList { get; set; }
}

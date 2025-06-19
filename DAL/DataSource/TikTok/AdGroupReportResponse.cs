using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.TikTok;

public class AdGroup
{
    [JsonProperty("adgroup_id")]
    public string AdGroupId { get; set; }
}

public class AdGroupReportList
{
    [JsonProperty("list")]
    public List<AdGroup> AdGroupIds { get; set; }
}

public class AdGroupReportResponse : BaseResponse
{
    [JsonProperty("data")]
    public AdGroupReportList AdGroupReportList { get; set; }
}

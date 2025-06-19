using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.TikTok;

public class Campaign
{
    [JsonProperty("campaign_id")]
    public string CampaignId { get; set; }
}

public class CampaignReportList
{
    [JsonProperty("list")]
    public List<Campaign> CampaignIds { get; set; }
}

public class CampaignReportResponse : BaseResponse
{
    [JsonProperty("data")]
    public CampaignReportList CampaignReportList { get; set; }
}

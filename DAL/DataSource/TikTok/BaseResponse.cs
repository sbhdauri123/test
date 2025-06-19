using Greenhouse.Data.DataSource.TikTok;
using Newtonsoft.Json;

namespace Greenhouse.DAL.DataSource.TikTok;

public abstract class BaseResponse
{
    [JsonProperty("page_info")]
    public PageInfo PageInfo { get; set; }

    [JsonProperty("code")]
    public int Code { get; set; }
}

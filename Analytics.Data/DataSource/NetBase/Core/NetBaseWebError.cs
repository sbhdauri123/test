using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.NetBase.Core
{
    public class NetBaseWebError
    {
        [JsonProperty("httpStatusCode")]
        public int HttpStatusCode { get; set; }
    }
}

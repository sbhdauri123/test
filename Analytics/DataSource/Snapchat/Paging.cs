using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public partial class Paging
    {
        [JsonProperty("next_link", NullValueHandling = NullValueHandling.Ignore)]
        public string next_page_link { get; set; }
    }
}

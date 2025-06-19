using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Skai
{
    [Serializable]
    public class QueryParameters
    {
        [JsonProperty("profile_id")]
        public string ProfileId { get; set; }
        [JsonProperty("ks")]
        public string ServerID { get; set; }
    }
}

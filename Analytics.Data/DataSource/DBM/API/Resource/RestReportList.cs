using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class RestReportList
    {
        [JsonProperty("reports")]
        public List<RestReport> Reports { get; set; }
        [JsonProperty("nextPageToken")]
        public string NextPageToken { get; set; }
    }
}

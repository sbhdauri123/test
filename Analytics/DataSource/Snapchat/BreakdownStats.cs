using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Snapchat
{
    public class BreakdownStats
    {
        [JsonProperty("ad")]
        public List<StatsBreakdown> AdBreakdown { get; set; }
        [JsonProperty("adsquad")]
        public List<StatsBreakdown> AdSquadBreakdown { get; set; }
    }
}

using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class TrueUpConfiguration
    {
        [JsonProperty("weeklyOffsetStart")]
        public string WeeklyOffsetStart { get; set; }

        [JsonProperty("weeklyOffsetEnd")]
        public string WeeklyOffsetEnd { get; set; }
    }
}

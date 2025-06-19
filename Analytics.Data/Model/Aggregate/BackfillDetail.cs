using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Aggregate
{
    [Serializable]
    public class BackfillDetail
    {
        [JsonProperty("sd")]
        public DateTime StartDate { get; set; }

        [JsonProperty("ed")]
        public DateTime EndDate { get; set; }

        [JsonProperty("cd")]
        public DateTime CreatedDate { get; set; }
    }
}

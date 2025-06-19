using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Greenhouse.Data.Model.DCM
{
    public class DimensionData
    {
        [JsonProperty("data")]
        public JArray Data { get; set; }
    }
}

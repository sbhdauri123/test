using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.Brief
{
    public class AarcEntity
    {
        [JsonProperty("mappedAgencyDivisionID")]
        public string MappedAgencyDivisionID { get; set; }
        [JsonProperty("mappedBusinessUnitID")]
        public string MappedBusinessUnitID { get; set; }
        [JsonProperty("mappedRegionID")]
        public string MappedRegionID { get; set; }
        [JsonProperty("mappedCountryID")]
        public string MappedCountryID { get; set; }
    }
}

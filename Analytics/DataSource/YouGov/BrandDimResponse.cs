using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.YouGov
{
    public class ValidityPeriod
    {
        [JsonProperty("start_date")]
        public string StartDate { get; set; }

        [JsonProperty("end_date")]
        public string EndDate { get; set; }
    }

    public class Brand
    {
        [JsonProperty("id")]
        public int ID { get; set; }

        [JsonProperty("region")]
        public string Region { get; set; }

        [JsonProperty("sector_id")]
        public int SectorId { get; set; }

        [JsonProperty("label")]
        public string Label { get; set; }

        [JsonProperty("is_active")]
        public bool IsActive { get; set; }

        [JsonProperty("validity_periods")]
        public List<ValidityPeriod> ValidityPeriods { get; set; }
    }

    public class BrandDimResponse
    {
        [JsonProperty("meta")]
        public Meta Meta { get; set; }

        [JsonProperty("data")]
        public Dictionary<string, Brand> Data { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.YouGov
{
    public class ReportRequest
    {
        [JsonProperty("data")]
        public ReportRequestData Data { get; set; }

        [JsonProperty("meta")]
        public ReportRequestMeta Meta { get; set; }
    }
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse); 
    public class Entity
    {
        [JsonProperty("region")]
        public string Region { get; set; }

        [JsonProperty("brands_from_sector_id")]
        public int BrandsFromSectorID { get; set; }
    }

    public class Period
    {
        [JsonProperty("start_date")]
        public dynamic StartDate { get; set; }
        [JsonProperty("end_date")]
        public dynamic EndDate { get; set; }
    }

    public class Query
    {
        [JsonProperty("entity")]
        public Entity Entity { get; set; }

        [JsonProperty("filters")]
        public dynamic Filters { get; set; }

        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("metrics_score_types")]
        public dynamic MetricsScoreTypes { get; set; }

        [JsonProperty("period")]
        public Period Period { get; set; }

        [JsonProperty("scoring")]
        public string Scoring { get; set; }
    }

    public class ReportRequestData
    {
        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("queries")]
        public List<Query> Queries { get; set; }

        [JsonProperty("title")]
        public string Title { get; set; }
    }

    public class ReportRequestMeta
    {
        [JsonProperty("version")]
        public string Version { get; set; }
    }
}

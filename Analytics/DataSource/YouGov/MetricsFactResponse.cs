using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.YouGov
{
    public class Coordinates
    {
        [JsonProperty("metric")]
        public List<string> Metric { get; set; }

        [JsonProperty("date")]
        public List<string> Date { get; set; }

        [JsonProperty("perspective")]
        public List<string> Perspective { get; set; }
    }

    public class FactData
    {
        [JsonProperty("dimensions")]
        public List<string> Dimensions { get; set; }

        [JsonProperty("coordinates")]
        public Coordinates Coordinates { get; set; }

        [JsonProperty("values")]
        public List<List<List<double?>>> Values { get; set; }
    }

    public class FactEntity
    {
        [JsonProperty("brand_id")]
        public string BrandID { get; set; }

        [JsonProperty("sector_id")]
        public string SectorID { get; set; }

        [JsonProperty("region")]
        public string Region { get; set; }
    }

    public class FactQuery
    {
        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("data")]
        public FactData Data { get; set; }

        [JsonProperty("entity")]
        public FactEntity Entity { get; set; }

        [JsonProperty("query_index")]
        public int QueryIndex { get; set; }

        [JsonProperty("periods")]
        public object Periods { get; set; }

        [JsonProperty("last_period_is_complete")]
        public bool LastPeriodIsComplete { get; set; }

        [JsonProperty("last_day_with_data")]
        public string LastDayWithData { get; set; }
    }

    public class ContainerData
    {
        [JsonProperty("id")]
        public string ID { get; set; }

        [JsonProperty("queries")]
        public List<FactQuery> Queries { get; set; }
    }

    public class MetricsFactResponse
    {
        [JsonProperty("meta")]
        public Meta Meta { get; set; }

        [JsonProperty("data")]
        public ContainerData Data { get; set; }
    }
}

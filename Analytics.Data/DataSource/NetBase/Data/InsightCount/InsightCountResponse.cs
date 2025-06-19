using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.NetBase.Data.InsightCount
{
    public class InsightCountResponse : Core.ReportResponse
    {
        [JsonProperty("insights")]
        public List<Insight> Insights { get; set; }
        [JsonProperty("errorCode")]
        public string ErrorCode { get; set; }
    }
    public class Insight
    {
        [JsonProperty("insightGroup")]
        public string InsightGroup { get; set; }
        [JsonProperty("dataset")]
        public List<InsightCountDataset> Dataset { get; set; }
    }
    public class InsightCountDataset
    {
        [JsonProperty("insightType")]
        public string InsightType { get; set; }
        [JsonProperty("set")]
        public List<Set> Set { get; set; }
    }
    public class Set
    {
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
        [JsonProperty("domain")]
        public string Domain { get; set; }
        [JsonProperty("authorName")]
        public string AuthorName { get; set; }
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("geoType")]
        public string GeoType { get; set; }
        [JsonProperty("parentId")]
        public string ParentId { get; set; }
        [JsonProperty("isoName")]
        public string IsoName { get; set; }
        [JsonProperty("stemmedForm")]
        public string StemmedForm { get; set; }
    }
}

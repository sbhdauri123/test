using Newtonsoft.Json;
namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class RestReport
    {
        [JsonProperty("key")]
        public ReportKey Key { get; set; }
        [JsonProperty("metadata")]
        public ReportMetadata Metadata { get; set; }
        [JsonProperty("params")]
        public ResourceParameters Params { get; set; }
    }
    public class ReportKey
    {
        [JsonProperty("queryId")]
        public string QueryId { get; set; }
        [JsonProperty("reportId")]
        public string ReportId { get; set; }
    }
    public class ReportMetadata
    {
        [JsonProperty("status")]
        public Status Status { get; set; }
        [JsonProperty("reportDataStartDate")]
        public ResourceDate ReportDataStartDate { get; set; }
        [JsonProperty("reportDataEndDate")]
        public ResourceDate ReportDataEndDate { get; set; }
        [JsonProperty("googleCloudStoragePath")]
        public string GoogleCloudStoragePath { get; set; }
    }
    public class Status
    {
        [JsonProperty("state")]
        public string State { get; set; }
        [JsonProperty("finishTime")]
        public string FinishTime { get; set; }
        [JsonProperty("format")]
        public string Format { get; set; }
    }
}

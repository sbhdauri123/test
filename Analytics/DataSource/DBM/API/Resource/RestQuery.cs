using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Greenhouse.Data.DataSource.DBM.API.Resource
{
    public class QueryMetadata
    {
        [JsonProperty("dataRange")]
        public QueryDataRange DataRange { get; set; }
        [JsonProperty("format"), JsonConverter(typeof(StringEnumConverter))]
        public ReportFormat Format { get; set; }
        [JsonProperty("title")]
        public string Title { get; set; }
    }

    public class QuerySchedule
    {
        [JsonProperty("frequency"), JsonConverter(typeof(StringEnumConverter))]
        public ReportFrequency Frequency { get; set; } = ReportFrequency.ONE_TIME;
        [JsonProperty("startDate")]
        public ResourceDate QueryScheduleStartDate { get; set; }
        [JsonProperty("endDate")]
        public ResourceDate QueryScheduleEndDate { get; set; }
    }

    public class RestQuery
    {
        [JsonProperty("queryId")]
        public string QueryId { get; set; }
        [JsonProperty("metadata")]
        public QueryMetadata Metadata { get; set; }
        [JsonProperty("params")]
        public ResourceParameters Params { get; set; }
        [JsonProperty("schedule")]
        public QuerySchedule Schedule { get; set; }
    }

    public class QueryDataRange
    {
        [JsonProperty("range"), JsonConverter(typeof(StringEnumConverter))]
        public DataRange Range { get; set; }
        [JsonProperty("customStartDate")]
        public ResourceDate QueryDataStartDate { get; set; }
        [JsonProperty("customEndDate")]
        public ResourceDate QueryDataEndDate { get; set; }
    }
}
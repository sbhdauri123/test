using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Euromonitor.Responses
{
    public class JobHistoryResponse
    {
        [JsonProperty("jobId")]
        public string JobId { get; set; }
        
        [JsonProperty("executionList")]
        public string ExecutionListString { get; set; }
        
        public ExecutionList ExecutionList { get; set; }

        [JsonProperty("processingStatus")]
        public string ProcessingStatus { get; set; }

        [JsonProperty("measureType")]
        public string MeasureType { get; set; }

        [JsonProperty("logDateTime")]
        public DateTime LogDateTime { get; set; }
    }

    public class ExecutionList
    {
        [JsonProperty("IndustryCode")]
        public string IndustryCode { get; set; }

        [JsonProperty("GeographyIds")]
        public int[] GeographyIds { get; set; }

        [JsonProperty("ShareTypeId")]
        public int ShareTypeId { get; set; }

        [JsonProperty("UnitType")]
        public string UnitType { get; set; }
    }
}

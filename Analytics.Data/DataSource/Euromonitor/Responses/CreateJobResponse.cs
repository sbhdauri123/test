using System;
using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor.Responses
{
    public class CreateJobResponse
    {
        [JsonPropertyName("jobId")]
        public string JobId { get; set; }

        [JsonPropertyName("processingStatus")]
        public string ProcessingStatus { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }

        public TimeSpan NextQuotaTime => TimeSpan.Parse(Message.Substring(Message.IndexOf("00:"), 8));
    }
}
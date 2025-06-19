using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor.Responses
{
    public class DownloadJobResponse
    {
        [JsonPropertyName("jobDownloadUri")]
        public string JobDownloadUri { get; set; }

        [JsonPropertyName("status")]
        public string Status { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }
    }
}

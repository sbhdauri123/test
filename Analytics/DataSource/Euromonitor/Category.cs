using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public class Category
    {
        [JsonPropertyName("industryCode")]
        public string IndustryCode { get; set; }
    }
}
using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public class Geography
    {
        [JsonPropertyName("id")]
        public int Id { get; set; }
    }
}

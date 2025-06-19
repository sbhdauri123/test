using System.Text.Json.Serialization;

namespace Greenhouse.Data.DataSource.Euromonitor.Requests
{
    public class DataTypeRequest
    {
        [JsonPropertyName("industryCodes")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string[] IndustryCodes { get; set; }
    }
}
using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.NetBase.Core
{
    public class DateParameterOverride
    {
        [JsonProperty("topicID")]
        public string TopicId { get; set; }
        [JsonProperty("useTopicSettings")]
        public bool UseTopicSettings { get; set; }
    }
}

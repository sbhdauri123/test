using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.NetBase.Data.Topics
{
    public class TopicsResponse : Core.ReportResponse
    {
        [JsonProperty("contentType")]
        public string ContentType { get; set; }
        [JsonProperty("createDate")]
        public string CreateDate { get; set; }
        [JsonProperty("description")]
        public string Description { get; set; }
        [JsonProperty("displayImgUrl")]
        public string DisplayImgUrl { get; set; }
        [JsonProperty("editDate")]
        public string EditDate { get; set; }
        [JsonProperty("fhTwitterStatus")]
        public string FhTwitterStatus { get; set; }
        [JsonProperty("fromDate")]
        public string FromDate { get; set; }
        [JsonProperty("languageFilters")]
        public List<string> LanguageFilters { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("owner")]
        public Owner Owner { get; set; }
        [JsonProperty("sharing")]
        public string Sharing { get; set; }
        [JsonProperty("status")]
        public string Status { get; set; }
        [JsonProperty("timeInterval")]
        public string TimeInterval { get; set; }
        [JsonProperty("toDate")]
        public string ToDate { get; set; }
        [JsonProperty("topicId")]
        public string TopicId { get; set; }
    }
    public class Owner
    {
        [JsonProperty("userProfileId")]
        public string UserProfileId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

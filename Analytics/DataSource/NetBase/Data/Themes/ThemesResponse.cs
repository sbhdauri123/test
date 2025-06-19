using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.NetBase.Data.Themes
{
    public class ThemesResponse : Core.ReportResponse
    {
        [JsonProperty("createdDate")]
        public string CreatedDate { get; set; }
        [JsonProperty("editedDate")]
        public string EditedDate { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
        [JsonProperty("owner")]
        public Owner Owner { get; set; }
        [JsonProperty("sharing")]
        public string Sharing { get; set; }
        [JsonProperty("tags")]
        public List<string> Tags { get; set; }
        [JsonProperty("themeId")]
        public string ThemeId { get; set; }
        [JsonProperty("userId")]
        public string UserId { get; set; }
    }
    public class Owner
    {
        [JsonProperty("userProfileId")]
        public string UserProfileId { get; set; }
        [JsonProperty("name")]
        public string Name { get; set; }
    }
}

using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.NetBase.Data.Themes
{
    public class ThemesParameters : Core.ReportSettings
    {
        [JsonProperty("categories")]
        public string Categories { get; set; }
        [JsonProperty("datetimeISO")]
        public string DatetimeISO { get; set; }
        [JsonProperty("ids")]
        public List<string> Ids { get; set; }
        [JsonProperty("pretty")]
        public string Pretty { get; set; }
        [JsonProperty("scope")]
        public string Scope { get; set; }

        public enum ContentTypeEnum
        {
            SocialWeb,
            Audience,
            A3DSupplement
        }

        public enum ScopeEnum
        {
            USER,
            ORG,
            GLOBAL
        }
    }
}

using Newtonsoft.Json;

namespace Greenhouse.Data.DataSource.NetBase.Data.InsightCount
{
    public class InsightCountParameters : Core.ReportSettings
    {
        #region required
        [JsonProperty("topicIds")]
        public string TopicIDs { get; set; }
        [JsonProperty("topics")]
        public string Topics { get; set; }
        [JsonProperty("categories")]
        public string Categories { get; set; }
        #endregion

        [JsonProperty("advancedFilter")]
        public string AdvancedFilter { get; set; }
        [JsonProperty("alertDatetime")]
        public string AlertDatetime { get; set; }
        [JsonProperty("alertTimestamp")]
        public string AlertTimestamp { get; set; }
        [JsonProperty("authors")]
        public string Authors { get; set; }
        [JsonProperty("dateRange")]
        public string DateRange { get; set; }
        [JsonProperty("dateRangeTimeZone")]
        public string DateRangeTimeZone { get; set; }
        [JsonProperty("datetimeISO")]
        public string DatetimeISO { get; set; }
        [JsonProperty("domains")]
        public string Domains { get; set; }
        [JsonProperty("excludedAuthors")]
        public string ExcludedAuthors { get; set; }
        [JsonProperty("excludeDomains")]
        public string ExcludeDomains { get; set; }
        [JsonProperty("excludeGeos")]
        public string ExcludeGeos { get; set; }
        [JsonProperty("excludedKeywords")]
        public string ExcludedKeywords { get; set; }
        [JsonProperty("genders")]
        public string Genders { get; set; }
        [JsonProperty("geos")]
        public string Geos { get; set; }
        [JsonProperty("hashTags")]
        public string HashTags { get; set; }
        [JsonProperty("insights")]
        public string Insights { get; set; }
        [JsonProperty("jsoncallback")]
        public string Jsoncallback { get; set; }
        [JsonProperty("keywords")]
        public string Keywords { get; set; }
        [JsonProperty("languageISO")]
        public string LanguageISO { get; set; }
        [JsonProperty("measure")]
        public string Measure { get; set; }
        [JsonProperty("orgProducts")]
        public string OrgProducts { get; set; }
        [JsonProperty("people")]
        public string People { get; set; }
        [JsonProperty("phrases")]
        public string Phrases { get; set; }
        [JsonProperty("precision")]
        public string Precision { get; set; }
        [JsonProperty("pretty")]
        public string Pretty { get; set; }
        [JsonProperty("publishedDate")]
        public string PublishedDate { get; set; }
        [JsonProperty("realTime")]
        public string RealTime { get; set; }
        [JsonProperty("sentiments")]
        public string Sentiments { get; set; }
        [JsonProperty("sizeNeeded")]
        public string SizeNeeded { get; set; }
        [JsonProperty("sources")]
        public string Sources { get; set; }
        [JsonProperty("themeIds")]
        public string ThemeIds { get; set; }
        [JsonProperty("timePeriod")]
        public string TimePeriod { get; set; }
        [JsonProperty("timePeriodOffset")]
        public string TimePeriodOffset { get; set; }
        [JsonProperty("timePeriodRounding")]
        public string TimePeriodRounding { get; set; }
        [JsonProperty("timestamp")]
        public string Timestamp { get; set; }

        public enum CategoriesEnum
        {
            None,
            Likes,
            Dislikes,
            Languages,
            PositiveEmotions,
            NegativeEmotions,
            PositiveBehaviors,
            NegativeBehaviors,
            Authors,
            Domains,
            Sources,
            Gender,
            Geolocation,
            Sentiment,
            Phrases,
            Hashtags,
            OrgProducts,
            People,
            Things
        }
    }
}

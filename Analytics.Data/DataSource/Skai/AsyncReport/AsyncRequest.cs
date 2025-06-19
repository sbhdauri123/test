using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Skai.AsyncReport
{
    [Serializable]
    public class AsyncRequest
    {
        [JsonProperty("template_name")]
        public string TemplateName { get; set; }
        [JsonProperty("start_date")]
        public string StartDate { get; set; }
        [JsonProperty("end_date")]
        public string EndDate { get; set; }
        [JsonProperty("currency")]
        public string Currency { get; set; }
        [JsonProperty("channels")]
        public IEnumerable<string> Channels { get; set; }
        [JsonProperty("fields")]
        public IEnumerable<Field> Fields { get; set; }
        [JsonProperty("custom_file_name")]
        public string CustomFileName { get; set; }
        [JsonProperty("compress_method")]
        public string CompressMethod { get; set; }
        [JsonProperty("delimiter")]
        public string Delimiter { get; set; }
        [JsonProperty("require_yesterday_performance")]
        public bool RequireYesterdayPerformance { get; set; }
        [JsonProperty("include_revenue_columns")]
        public bool IncludeRevenueColumns { get; set; }
        [JsonProperty("entity_level")]
        public string EntityLevel { get; set; }
        [JsonProperty("dimensions")]
        public string Dimensions { get; set; }
    }
    public class Field
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("group")]
        public string Group { get; set; }
    }
}

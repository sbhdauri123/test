using Greenhouse.Data.DataSource.Skai.AsyncReport;
using Greenhouse.Data.Model.Setup;
using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Skai
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public ReportType ReportType { get; set; }

        [JsonProperty("method")]
        public string Method { get; set; }

        [JsonProperty("path")]
        public string Path { get; set; }

        [JsonProperty("asyncRequestJson")]
        public AsyncRequest AsyncRequest { get; set; }

        [JsonProperty("scheduleSetting")]
        public AggregateInitializeSettings ScheduleSetting { get; set; }

        [JsonProperty("entity")]
        public ReportEntity Entity { get; set; }

        [JsonProperty("pageLimit")]
        public int PageLimit { get; set; } = 1000;
    }

    public enum ReportType
    {
        None,
        fusion,
        custom,
        column
    }

    public enum ReportEntity
    {
        None,
        CAMPAIGN,
        ADGROUP,
        KEYWORD,
        AD
    }
}

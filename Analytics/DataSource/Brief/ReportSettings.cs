using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.Brief
{
    [Serializable]
    public class ReportSettings
    {
        [JsonProperty("reportName")]
        public string ReportName { get; set; }

        [JsonProperty("reportType")]
        public string ReportType { get; set; }

        [JsonProperty("entityName")]
        public string EntityName { get; set; }

        [JsonProperty("method")]
        public string Method { get; set; }

        [JsonProperty("path")]
        public string Path { get; set; }

        [JsonProperty("extension")]
        public string Extension { get; set; }

        [JsonProperty("parameters")]
        public string Parameters { get; set; }

        [JsonProperty("pageSize")]
        public int PageSize { get; set; }

        // stages raw data and names the json object array
        // by adding {"allData":<raw-json>}
        // this is to help copy JSON data into Redshift
        [JsonProperty("stageJsonArray")]
        public bool StageJsonArray { get; set; }
        [JsonProperty("objectListSize")]
        public int objectListSize { get; set; }
    }
}

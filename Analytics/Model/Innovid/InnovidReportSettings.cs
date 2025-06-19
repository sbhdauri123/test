using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.Model.Innovid
{
    [Serializable]
    public class InnovidReportSettings
    {
        [JsonProperty("reportName")]
        public string ReportName { get; set; }

        [JsonProperty("reportType")]
        public string ReportType { get; set; }
    }
}

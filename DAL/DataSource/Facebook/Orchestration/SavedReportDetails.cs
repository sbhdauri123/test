using Greenhouse.Data.DataSource.Facebook;
using Newtonsoft.Json;
using System;

namespace Greenhouse.DAL.DataSource.Facebook.Orchestration
{
    [Serializable]
    public class SavedReportDetails
    {
        [JsonProperty("e")]
        public string EntityID { get; set; }

        [JsonProperty("t")]
        public string ReportName { get; set; }

        [JsonProperty("l")]
        public string ReportLevel { get; set; }

        [JsonProperty("i")]
        public string ReportID { get; set; }

        [JsonProperty("u")]
        public string Url { get; set; }

        [JsonProperty("r")]
        public bool Resubmit { get; set; }

        public SavedReportDetails(FacebookReportItem reportItem)
        {
            if (reportItem != null)
            {
                EntityID = reportItem.EntityID;
                ReportName = reportItem.ReportName;
                ReportLevel = reportItem.ReportLevel;
                ReportID = reportItem.ReportRunId;
                Url = reportItem.OriginalInsightsUrl;
                Resubmit = reportItem.DownloadFailed || reportItem.StatusCheckFailed;
            }
        }
    }
}

using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.FRED
{
    public class ReportSettings
    {
        [JsonProperty("reportType")]
        public string ReportType { get; set; }
        [JsonProperty("reportEntity")]
        public string ReportEntity { get; set; }
        [JsonProperty("URLPath")]
        public string URLPath { get; set; }
        [JsonProperty("reportScheduleDetails")]
        public ICollection<ReportSchedule> ReportScheduleDetails { get; set; }

        [Serializable]
        public class ReportSchedule
        {
            [JsonProperty("triggerDay")]
            public string TriggerDayRegex { get; set; }
            [JsonProperty("interval")]
            public IntervalEnum Interval { get; set; }
            [JsonProperty("priority")]
            public int Priority { get; set; }
        }

        public enum IntervalEnum { Every, First, Last, Daily, LastDayOfTheMonth }
    }
}

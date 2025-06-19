using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.DBM.API.Core
{
    public class ScheduledReport
    {
        [JsonProperty("daily_reports")]
        public List<Reports> DailyReports { get; set; }
        [JsonProperty("backfill_reports")]
        public List<Reports> BackfillReports { get; set; }
    }
    public class Reports
    {
        [JsonProperty("partner_id")]
        public long PartnerId { get; set; }
        [JsonProperty("query_id")]
        public List<int> QueryIdList { get; set; }
    }
}

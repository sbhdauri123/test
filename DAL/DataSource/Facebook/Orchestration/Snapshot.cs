using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.DAL.DataSource.Facebook.Orchestration
{
    public class Snapshot
    {
        [JsonProperty("id")]
        public long QueueID { get; set; }
        [JsonProperty("savedReports")]
        public List<SavedReportDetails> PendingReports { get; set; } = new List<SavedReportDetails>();
        public IEnumerable<SavedReportDetails> ReportsToResubmit
        {
            get { return PendingReports.Where(r => r.Resubmit == true); }
        }
        public IEnumerable<SavedReportDetails> ReportsToCheckStatus
        {
            get { return PendingReports.Where(r => r.Resubmit == false); }
        }
        public DateTime SnapshotDate { get; set; }
        public List<string> GetDimensionIdList(ListAsset listAsset)
        {
            var idList = new List<string>();

            // empty reports will have no entity ID
            var entityIdList = PendingReports.Where(r => Utilities.UtilsText.ConvertToEnum<ListAsset>(r.ReportLevel) == listAsset && !string.IsNullOrEmpty(r.EntityID)).Select(x => x.EntityID).Distinct();

            if (entityIdList.Any())
                idList.AddRange(entityIdList);

            return idList;
        }
    }
}

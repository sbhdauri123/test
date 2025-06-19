using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook.Core
{
    public class QueueDateTracker
    {
        public List<QueueInfo> QueueInfoList { get; set; }
    }
    public class QueueInfo
    {
        public long QueueID { get; set; }
        public string EntityID { get; set; }
        // IsPrimary is most recent file-date where processing is attributed (-1 offset)
        public bool IsPrimaryDate { get; set; }
    }
}

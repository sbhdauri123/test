using System;

namespace Greenhouse.Data.DataSource.DataCertification
{
    [Serializable]
    public class BackfillQueue
    {
        public string BackfillKey { get; set; }
        public int SourceID { get; set; }
        public int IntegrationID { get; set; }
        public string EntityID { get; set; }
        public DateTime FileDate { get; set; }
        public bool BackfillScheduled { get; set; }
        public string Fileguid { get; set; }
        public Int64 QueueID { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime LastUpdated { get; set; }
        public int? EntityPriorityOrder { get; set; }
        public bool IsCurrentQueue { get; set; }
    }
}

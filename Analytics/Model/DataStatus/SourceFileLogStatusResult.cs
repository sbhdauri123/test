using System;

namespace Greenhouse.Data.Model.DataStatus
{
    public class SourceFileLogStatusResult
    {
        public int SourceID { get; set; }
        public string SourceName { get; set; }
        public string EntityId { get; set; }
        public int ImportCountDown { get; set; }
        public int ProcessingCount { get; set; }
        public int TotalCount { get; set; }
        public DateTime? StatusDate { get; set; }
        public DateTime? ExpectedDate { get; set; }
        public DateTime? MaxFileDate { get; set; }
        public int IntegrationID { get; set; }
    }
}

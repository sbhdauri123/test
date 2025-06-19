namespace Greenhouse.UI.Models
{
    [Serializable]
    public class JobQueueModel
    {
        public Int64 JobLogID { get; set; }
        //public string SourceName { get; set; }
        //public string IntegrationName { get; set; }
        public string FileName { get; set; }
        public string Status { get; set; }
        public DateTime LastUpdated { get; set; }
    }
}
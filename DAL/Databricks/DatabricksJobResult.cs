namespace Greenhouse.DAL.Databricks
{
    public class DatabricksJobResult
    {
        public long QueueID { get; set; }
        public long JobRunID { get; set; }
        public ResultState JobStatus { get; set; }
    }
}

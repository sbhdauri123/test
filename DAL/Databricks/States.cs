namespace Greenhouse.DAL.Databricks
{
    public enum ResultState
    {
        WAITING,
        SUCCESS,
        FAILED,
        CANCELED,
        QUEUED,
        SKIPPED,
        NONE
    }
}

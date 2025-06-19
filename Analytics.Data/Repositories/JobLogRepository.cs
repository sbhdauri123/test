using Dapper;

namespace Greenhouse.Data.Repositories
{
    public class JobLogRepository : BaseRepository<Greenhouse.Data.Model.Core.JobLog>
    {
        public int DeleteJobLogs(long jobLogID, string jobStatus)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@JobLogID", jobLogID);
            parameters.Add("@status", jobStatus);
            int retValue = ExecuteStoredProc("DeleteJobLog", parameters);
            return retValue;
        }
    }
}

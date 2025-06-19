using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;

namespace Greenhouse.Data.Repositories
{
    public class DatabricksJobLogRepository : IDatabricksJobLogRepository
    {
        public void UpdateDatabricksJobLog(long queueID, long runID, string status, long jobID, string jobParameters = null)
        {
            var baseRepository = new BaseRepository<DatabricksJobLog>();

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@QueueID", queueID);
            parameters.Add("@RunID", runID);
            parameters.Add("@Status", status);
            parameters.Add("@DatabricksJobID", jobID);
            parameters.Add("@DatabricksJobParameters", jobParameters);

            baseRepository.QueryStoredProc("UpdateDatabricksJobLog", parameters);
        }

        public IEnumerable<DatabricksJobLog> GetDatabricksQueueJobLog(long queueID, int nbResults = 1)
        {
            var baseRepository = new BaseRepository<DatabricksJobLog>();

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@QueueID", queueID);
            parameters.Add("@NbResults", nbResults);

            return baseRepository.QueryStoredProc("GetDatabricksQueueJobLog", parameters);
        }

        public IEnumerable<DatabricksJobLog> GetDatabricksJobLogs(long integrationID, long jobLogID)
        {
            var baseRepository = new BaseRepository<DatabricksJobLog>();

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@IntegrationID", integrationID);
            parameters.Add("@JobLogID", jobLogID);

            return baseRepository.QueryStoredProc("GetDatabricksJobLogs", parameters);
        }
    }
}

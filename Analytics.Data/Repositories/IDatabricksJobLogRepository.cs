using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;

namespace Greenhouse.Data.Repositories
{
    public interface IDatabricksJobLogRepository
    {
        public void UpdateDatabricksJobLog(long queueID, long runID, string status, long jobID, string jobParameters = null);
        public IEnumerable<DatabricksJobLog> GetDatabricksQueueJobLog(long queueID, int nbResults = 1);
        public IEnumerable<DatabricksJobLog> GetDatabricksJobLogs(long integrationID, long jobLogID);
    }
}
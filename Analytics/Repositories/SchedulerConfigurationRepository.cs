using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class SchedulerConfigurationRepository : BaseRepository<Model.Setup.SchedulerConfiguration>
    {
        public IEnumerable<Model.Setup.SchedulerConfiguration> GetSchedulerConfigByJobType(int jobTypeId)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.SchedulerConfiguration>(string.Format("SELECT * FROM SchedulerConfiguration where JobTypeID = @jobTypeID "), new { jobTypeId = jobTypeId });
            }
        }
    }
}

using Dapper;
using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class SourceRepository : BaseRepository<Model.Setup.Source>
    {
        public IEnumerable<Model.Setup.Source> GetSources(int jobStepId)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.Source>("SELECT DISTINCT a.SourceID, a.SourceName, a.AggregateInitializeSettings FROM Source a JOIN SourceJob b ON a.SourceID = b.SourceID WHERE a.IsActive = 1 AND b.SourceJobStepID = @JobStepID", new { JobStepID = jobStepId });
            }
        }

        public IEnumerable<SourceInfo> GetAllSourceInfo()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetAllSourceInfo]", null, null, 6000,
                    CommandType.StoredProcedure);
                return connection.Query<Model.Setup.SourceInfo>(cmd);
            }
        }

        public IEnumerable<Source> GetAllSourceID()
        {
            var sql = "SELECT DISTINCT SourceID FROM Source;";
            return GetItems(sql);
        }
    }
}

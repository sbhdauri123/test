using Dapper;
using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class SourceJobRepository : BaseRepository<SourceJob>
    {
        public IEnumerable<SourceJob> GetMappedSourceJobs(int? sourceId, int executionTypeID)
        {
            using (IDbConnection connection = OpenConnection())
            {
                if (!sourceId.HasValue)
                {
                    string sql = "SELECT * FROM SourceJob sj INNER JOIN SourceJobStep sjs on sjs.SourceJobStepID = sj.SourceJobStepID WHERE sj.ExecutionTypeID=@ExecutionTypeID AND sj.SourceID IS NULL AND sjs.IsActive=1 ORDER BY sj.StepOrderIndex";
                    return connection.Query<SourceJob, SourceJobStep, SourceJob>(sql, (sj, sjs) => { sj.JobStep = sjs; return sj; }, new { ExecutionTypeID = executionTypeID }, splitOn: "SourceJobStepID");
                }
                else
                {
                    string sql = "SELECT * FROM SourceJob sj INNER JOIN SourceJobStep sjs on sjs.SourceJobStepID = sj.SourceJobStepID WHERE sj.ExecutionTypeID=@ExecutionTypeID AND sj.SourceID=@SourceID AND sjs.IsActive=1 ORDER BY sj.StepOrderIndex";
                    return connection.Query<SourceJob, SourceJobStep, SourceJob>(sql, (sj, sjs) => { sj.JobStep = sjs; return sj; }, new { SourceID = sourceId, ExecutionTypeID = executionTypeID }, splitOn: "SourceJobStepID");
                }
            }
        }

        public IEnumerable<string> GetSourceJobNames(int sourceId)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<string>("SELECT DISTINCT SourceJobName FROM SourceJob WHERE SourceID=@SourceID AND IsActive=1", new { SourceID = sourceId });
            }
        }

        public IEnumerable<SourceJob> GetSourceJobs(int sourceID, int sourceJobStepID)
        {
            using (IDbConnection connection = OpenConnection())
            {
                if (sourceID != 0)
                    return connection.Query<SourceJob>("SELECT * FROM SourceJob WHERE ISNULL(SourceID, 0) = @SourceID AND SourceJobStepID = @SourceJobStepID", new { SourceID = sourceID, SourceJobStepID = sourceJobStepID });
                else
                    return connection.Query<SourceJob>("SELECT * FROM SourceJob WHERE SourceJobStepID = @SourceJobStepID", new { SourceJobStepID = sourceJobStepID });
            }
        }
    }
}

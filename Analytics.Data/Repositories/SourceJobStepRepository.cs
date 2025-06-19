using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class SourceJobStepRepository : BaseRepository<Model.Setup.SourceJobStep>
    {
        public IEnumerable<Model.Setup.SourceJobStep> GetSourceJobStepTypes(int jobTypeID)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.SourceJobStep>("select distinct   a.SourceJobStepID, case when a.JobTypeID = 2 then s.SourceName else a.ShortDescription end as ShortDescription, a.IsBatch from SourceJobStep a join SourceJob b on a.SourceJobStepID = b.SourceJobStepID left join Source s on s.SourceID = b.SourceID where b.StepOrderIndex = 0 AND a.IsActive = 1 AND a.JobTypeID = @JobTypeID order by ShortDescription ", new { JobTypeID = jobTypeID });
            }
        }
    }
}

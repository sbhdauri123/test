using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class AdTagJobRunRepository : AdTagBaseRepository<Model.AdTag.JobRun>
    {
        new public IEnumerable<Model.AdTag.JobRun> GetAll()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.AdTag.JobRun>("select *, case when [Status] = 'Error' then 1 when [Status] = 'Running' then 2 when [Status] = 'Complete' then 3 else 99 end as StatusSortOrder from JobRun order by StatusSortOrder, LastUpdated desc");
            }
        }

        new public void Delete(Model.AdTag.JobRun entityToDelete)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Delete<Model.AdTag.JobRun>(entityToDelete);
            }
        }
    }
}
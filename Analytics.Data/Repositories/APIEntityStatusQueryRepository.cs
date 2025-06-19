using Dapper;

using Greenhouse.Data.Model.Setup;

using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class APIEntityStatusQueryRepository : BaseRepository<APIEntityStatusQuery>
    {
        public IEnumerable<APIEntityStatusQuery> GetQueries()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<APIEntityStatusQuery>("SELECT a.SourceID, RedshiftQuery, SourceName FROM APIEntityStatusQueries a INNER JOIN Source b on a.SourceID = b.SourceID;");
            }
        }
    }
}

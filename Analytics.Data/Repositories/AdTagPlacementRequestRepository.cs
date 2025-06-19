using Dapper;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class AdTagPlacementRequestRepository : AdTagBaseRepository<Model.AdTag.PlacementRequest>
    {
        public new int? Add(Model.AdTag.PlacementRequest placementRequest)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Insert<int, Model.AdTag.PlacementRequest>(placementRequest);
            }
        }
    }
}
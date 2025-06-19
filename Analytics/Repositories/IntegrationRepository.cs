using Dapper;
using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class IntegrationRepository : BaseRepository<Integration>
    {
        public IEnumerable<Integration> GetActiveIntegrations()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Integration>("SELECT IntegrationID, IntegrationName, SourceId FROM Integration WHERE IsActive = 1");
            }
        }

        /// <summary>
        /// Returns following columns: IntegrationID, IntegrationName, SourceId, and IsActive
        /// <returns>
        /// list of integrations
        /// </returns>
        public IEnumerable<Integration> GetAllIntegrations()
        {
            var sql = $"SELECT  IntegrationID, IntegrationName, SourceId, IsActive FROM Integration;";
            return GetItems(sql);
        }
    }
}

using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class MetadataPartitionRepository : BaseRepository<Model.Setup.MetadataPartition>
    {
        /// <summary>
        /// List of agency metastore records when no partition are added for more 2 hours.
        /// </summary>
        /// <returns> </returns>
        public IEnumerable<Model.Setup.MetadataPartition> GetAgencyMetastoreStatus()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetAgencyMetastoreStatus]", null, null, 6000, CommandType.StoredProcedure);
                return connection.Query<Model.Setup.MetadataPartition>(cmd);
            }
        }
    }
}

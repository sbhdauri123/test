using Dapper;
using System;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class HivePartitionRepository : BaseRepository<Greenhouse.Data.Model.Setup.HivePartition>
    {
        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Advertiser Mapping UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public IEnumerable<Model.Setup.HivePartition> GetNewPartitions()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetNewPartitions]", null, null, 6000, CommandType.StoredProcedure);
                return connection.Query<Model.Setup.HivePartition>(cmd);
            }
        }

        public IEnumerable<Model.Setup.HivePartition> GetNewAdvertiserPartitions()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetNewAdvertiserPartitions]", null, null, 6000, CommandType.StoredProcedure);
                return connection.Query<Model.Setup.HivePartition>(cmd);
            }
        }

        /// <summary>
        /// batch update partitions
        /// </summary>
        /// <param name="keys">Expected Tuple keys are AdvertiserMappingID,PartitionPath and IsPMInstance</param>
        /// <returns></returns>
        public int UpdatePartitions(IEnumerable<Tuple<long, string, bool>> keys, bool isPMinstance = false, int sqlCommandTimeout = 90)
        {
            int result = 0;

            DataTable partitionPathTable = CreatePartitionPathTable();
            foreach (Tuple<long, string, bool> key in keys)
            {
                partitionPathTable.Rows.Add(key.Item1, key.Item2);
            }

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@IsPMinstance", isPMinstance ? 1 : 0);
            parameters.Add("@Partitions", partitionPathTable, DbType.Object);

            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[UpdatePartitionIsAddedToMetastore]", parameters, null, sqlCommandTimeout, CommandType.StoredProcedure);
                result = connection.ExecuteScalar<int>(cmd);
            }
            return result;
        }

        public int GetConcurrentPuttySessions()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetNumberOfPuttySessions]", null, null, 6000, CommandType.StoredProcedure);
                return (int)connection.ExecuteScalar(cmd);
            }
        }

        private static DataTable CreatePartitionPathTable()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("PartitionID", typeof(string));
            dt.Columns.Add("PartitionPath", typeof(string));
            return dt;
        }
    }
}

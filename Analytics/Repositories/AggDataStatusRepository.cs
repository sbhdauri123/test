using Dapper;
using Greenhouse.Data.Model.DataStatus;
using Greenhouse.Data.Model.Setup;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class AggDataStatusRepository : BaseRepository<AggDataStatus>
    {
        public void UpdateAggDataTables(int sourceID,
            string jobGuid)
        {
            string transactionName = jobGuid.Replace("-", string.Empty);

            using (SqlConnection connection = (SqlConnection)OpenConnection())
            {
                using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted, transactionName))
                {
                    try
                    {
                        //calling SP that will update AgDataStatus based on the data inserted in the temp table
                        CallUpdateAggDataStatus(sourceID, connection, trans);
                        trans.Commit();
                    }
                    catch (Exception)
                    {
                        try
                        {
                            trans.Rollback(transactionName);
                        }
                        catch
                        {
                            // ignored exception if transaction is not usable
                        }

                        throw;
                    }
                }
            }
        }

        private void CallUpdateAggDataStatus(int sourceID, SqlConnection conn, SqlTransaction trans)
        {
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@SourceID", sourceID);
            base.ExecuteStoredProc(conn, "UpdateAggDataStatus", parameters, trans, 300);
        }

        public IEnumerable<AggDataStatusEmail> CallGetAggDataStatusForEmail()
        {
            using (SqlConnection connection = (SqlConnection)OpenConnection())
            {
                var cmd = new CommandDefinition("[dbo].[GetAggDataStatusForEmail]", null, null, 6000,
                    CommandType.StoredProcedure);
                return connection.Query<Model.DataStatus.AggDataStatusEmail>(cmd);
            }
        }
    }
}
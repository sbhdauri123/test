using Dapper;
using Dapper.Contrib.Extensions;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Greenhouse.Data.Repositories;

public class QueueRepository : BaseRepository<Greenhouse.Data.Model.Core.Queue>
{
    public IEnumerable<IFileItem> GetQueueProcessing(long integrationID, long jobLogID, bool isBackfill = false)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<Queue>("GetQueueProcessing", new { IntegrationID = integrationID, JobLogID = jobLogID, IsBackfill = isBackfill }, commandType: CommandType.StoredProcedure);
        }
    }

    public IEnumerable<OrderedQueue> GetFailedDatabricksJobRunQueueItems(long integrationID, long jobLogID)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<OrderedQueue>("GetFailedDatabricksJobRunQueueItems", new { IntegrationID = integrationID, JobLogID = jobLogID }, commandType: CommandType.StoredProcedure);
        }
    }

    public IOrderedEnumerable<OrderedQueue> GetOrderedQueueProcessing(long integrationID, long jobLogID, bool isBackfill = false)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<OrderedQueue>("GetOrderedQueueProcessing", new { IntegrationID = integrationID, JobLogID = jobLogID, IsBackfill = isBackfill }, commandType: CommandType.StoredProcedure)
                .OrderBy(x => x.RowNumber);
        }
    }

    public IOrderedEnumerable<OrderedQueue> GetOrderedQueueProcessingBySource(long sourceID, long jobLogID, bool isBackfill = false, int nbResults = 200)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<OrderedQueue>("GetOrderedQueueProcessingBySource", new { SourceID = sourceID, JobLogID = jobLogID, IsBackfill = isBackfill, NbResults = nbResults }, commandType: CommandType.StoredProcedure)
                .OrderBy(x => x.RowNumber);
        }
    }

    public bool IsBackfillQueue(int integrationID, int offsetHours)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return (connection.ExecuteScalar<int>(string.Format("SELECT TOP 1 1 FROM  Queue WHERE IntegrationID = @id AND FileDate < @datetime"), new { id = integrationID, datetime = DateTime.Now.AddHours((-1 * offsetHours)) }) != 0);
        }
    }

    public IEnumerable<int> GetActiveQueueIntegrations()
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<int>("GetActiveQueueIntegrations");
        }
    }

    public IOrderedEnumerable<OrderedQueue> GetTopQueueItemsBySource(int sourceId, int nbResults, long jobLogID, int? integrationID = null)
    {
        using (IDbConnection connection = OpenConnection())
        {
            var result = connection.Query<OrderedQueue>("GetTopQueueItemsBySource", new { NbResults = nbResults, SourceID = sourceId, IntegrationID = integrationID, JobLogID = jobLogID }, commandType: CommandType.StoredProcedure);
            return result.OrderBy(x => x.RowNumber);
        }
    }

    public IEnumerable<Queue> GetOrderedTopQueueItemsBySource(int sourceId, int nbResults, long jobLogID, int? integrationID = null)
    {
        using (IDbConnection connection = OpenConnection())
        {
            var result = connection.Query<Queue>("GetOrderedTopQueueItemsBySource", new { NbResults = nbResults, SourceID = sourceId, IntegrationID = integrationID, JobLogID = jobLogID }, commandType: CommandType.StoredProcedure);
            return result;
        }
    }

    /// <summary>
    /// KEEP THIS METHOD INTERNAL, it needs to be called through a locking mechanism
    /// If multiple instances of a job class call it simultaneously it could return a same queue
    /// </summary>
    /// <returns></returns>
    internal IEnumerable<OrderedQueue> GetOrderedTopQueueItemsByCredential(int sourceId, int nbResults, long jobLogID, int credentialID, int parentIntegrationID)
    {
        using (IDbConnection connection = OpenConnection())
        {
            var result = connection.Query<OrderedQueue>("GetOrderedTopQueueItemsByCredential", new { NbResults = nbResults, SourceID = sourceId, CredentialID = credentialID, JobLogID = jobLogID, ParentIntegrationID = parentIntegrationID }, commandType: CommandType.StoredProcedure);
            return result;
        }
    }

    public IFileItem GetByFileGUID(Guid guid)
    {
        using (IDbConnection connection = OpenConnection())
        {
            var items = connection.Query<Queue>(string.Format("SELECT * FROM Queue where FileGUID = @fileGUID "), new { fileGUID = guid });
            return items.SingleOrDefault();
        }
    }
    public Dictionary<int, int> GetQueueCountByIntegrationID(int sourceId)
    {
        var result = new Dictionary<int, int>();
        using (IDbConnection connection = OpenConnection())
        {
            var query = connection.Query("GetQueueCountByIntegrationID", new { SourceID = sourceId }, commandType: CommandType.StoredProcedure);
            foreach (var item in query)
            {
                int integrationId = (int)item.IntegrationId; // Cast to int
                int count = (int)item.Count; // Cast to int
                result.Add(integrationId, count);
            }
            return result;
        }
    }
    public IEnumerable<Queue> GetQueueItemsByIdList(List<long> queueIdList, int sourceId, long jobLogID)
    {
        DataTable tvpQueueIdTable = CreateTvpQueueIdTable();
        queueIdList.ForEach(id => tvpQueueIdTable.Rows.Add(id));
        var parameters = new Dapper.DynamicParameters();
        parameters.Add("@SourceID", sourceId);
        parameters.Add("@JobLogID", jobLogID);
        parameters.Add("@QueueIdList", tvpQueueIdTable, DbType.Object);
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<Queue>("GetQueueItemsByIdList", parameters, commandType: CommandType.StoredProcedure);
        }
    }

    public IEnumerable<Queue> GetQueueItemsByFileDate(DateTime fileDateStart, DateTime fileDateEnd, int sourceId, string APIEntityCode)
    {
        var parameters = new Dapper.DynamicParameters();
        parameters.Add("@SourceID", sourceId);
        parameters.Add("@APIEntityCode", APIEntityCode);
        parameters.Add("@FileDateStart", fileDateStart);
        parameters.Add("@FileDateEnd", fileDateEnd);
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<Queue>("GetQueueItemsByFileDate", parameters, commandType: CommandType.StoredProcedure);
        }
    }

    private static DataTable CreateTvpQueueIdTable()
    {
        DataTable dt = new DataTable();
        dt.Columns.Add("QueueID", typeof(long));
        return dt;
    }

    public bool AddToQueueWithTransaction<T>(IEnumerable<T> files)
    {
        bool status = false;

        using (IDbConnection connection = OpenConnection())
        {
            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                try
                {
                    connection.Insert<T>(files, trans);
                    trans.Commit();
                    status = true;
                }
                catch (Exception)
                {
                    trans.Rollback();
                    throw;
                }
            }
        }
        return status;
    }

    [Obsolete]
    public void BulkInsert(IEnumerable<Queue> inserts)
    {
        if (inserts == null || !inserts.Any())
        {
            return;
        }

        foreach (var insert in inserts)
        {
            insert.CreatedDate = DateTime.Now;
            insert.LastUpdated = DateTime.Now;
        }

        using (IDbConnection connection = OpenConnection())
        {
            var dtExt = new Utilities.DataTables.ObjectShredder<Queue>();
            var dataTable = dtExt.Shred(inserts, null, null);

            using (var sbc = new SqlBulkCopy((SqlConnection)connection, SqlBulkCopyOptions.FireTriggers, null))
            {
                //the column mappings are not generated dynamically because 2 properties/columns case are not matching (IsBackfill/IsBackFill and StatusId/StatusID)
                sbc.ColumnMappings.Add("ID", "ID");
                sbc.ColumnMappings.Add("JobLogID", "JobLogID");
                sbc.ColumnMappings.Add("FileGUID", "FileGUID");
                sbc.ColumnMappings.Add("Step", "Step");
                sbc.ColumnMappings.Add("IntegrationID", "IntegrationID");
                sbc.ColumnMappings.Add("SourceID", "SourceID");
                sbc.ColumnMappings.Add("SourceFileName", "SourceFileName");
                sbc.ColumnMappings.Add("FileName", "FileName");
                sbc.ColumnMappings.Add("EntityID", "EntityID");
                sbc.ColumnMappings.Add("FileDate", "FileDate");
                sbc.ColumnMappings.Add("FileDateHour", "FileDateHour");
                sbc.ColumnMappings.Add("StatusId", "StatusID");
                sbc.ColumnMappings.Add("Status", "Status");
                sbc.ColumnMappings.Add("FileSize", "FileSize");
                sbc.ColumnMappings.Add("FileCollectionJSON", "FileCollectionJSON");
                sbc.ColumnMappings.Add("IsBackfill", "IsBackFill");
                sbc.ColumnMappings.Add("DeliveryFileDate", "DeliveryFileDate");
                sbc.ColumnMappings.Add("CreatedDate", "CreatedDate");
                sbc.ColumnMappings.Add("LastUpdated", "LastUpdated");
                sbc.ColumnMappings.Add("IsDimOnly", "IsDimOnly");

                sbc.BulkCopyTimeout = 1000;
                sbc.BatchSize = 1000;

                sbc.DestinationTableName = "Queue";
                sbc.WriteToServer(dataTable);
            }

            dataTable.Clear();
            dataTable.Dispose();
        }
    }

    public IEnumerable<long> GetQueueIDBySource(int sourceId, int? integrationId)
    {
        DataTable tvpQueueIdTable = CreateTvpQueueIdTable();
        var parameters = new Dapper.DynamicParameters();
        parameters.Add("@SourceID", sourceId);
        parameters.Add("@IntegrationID", integrationId);
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<long>("GetQueueIDBySource", parameters, commandType: CommandType.StoredProcedure);
        }
    }

    public DateTime? GetMaxFileDateProcessedComplete(int integrationID, string entityID)
    {
        var parameters = new Dapper.DynamicParameters();
        parameters.Add("@IntegrationID", integrationID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
        parameters.Add("@EntityID", entityID, System.Data.DbType.String, System.Data.ParameterDirection.Input);
        using (IDbConnection connection = OpenConnection())

        {
            IEnumerable<DateTime?> result = connection.Query<DateTime?>("GetMaxFileDateProcessedComplete", parameters, commandType: CommandType.StoredProcedure);
            if (!result.Any())
            {
                return null;
            }

            return result.FirstOrDefault();
        }
    }

    public IEnumerable<Guid> GetQueueGuidBySource(int sourceId)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<Guid>(string.Format("SELECT DISTINCT FileGuid FROM Queue WITH(NOLOCK) WHERE SourceID = @SourceID"), new { SourceID = sourceId });
        }
    }

    public IEnumerable<Guid> GetQueueGuidByIntegration(int integrationId)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<Guid>(string.Format("SELECT DISTINCT FileGuid FROM Queue WITH(NOLOCK) WHERE IntegrationID = @IntegrationID"), new { IntegrationID = integrationId });
        }
    }

    /// <summary>
    /// Returns following columns: ID, FileGUID, SourceID, IntegrationID, EntityID, FileDate, Step, Status and IsBackFill
    /// </summary>
    /// <param name="sourceId"></param>
    /// <returns></returns>
    public IEnumerable<Queue> GetAllQueuesDataLight(int sourceId)
    {
        var sql = $"SELECT ID, FileGUID, SourceID, IntegrationID, EntityID, FileDate, Step, Status, IsBackFill FROM Queue WHERE SourceID = {sourceId};";
        return GetItems(sql);
    }
}

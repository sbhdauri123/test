using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using Generic = Greenhouse.Data.Model.Aggregate;

namespace Greenhouse.Data.Services
{
    /// <summary>
    /// Job-related database CRUD operations
    /// </summary>
    public static class JobService
    {
        private const string SP_NAME_GET_LATEST_DELIVERY_FILEDATE = "GetLatestDeliveryFileDate";

        public static IEnumerable<TEntity> GetAll<TEntity>()
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetAll();
        }

        public static IEnumerable<TEntity> GetAll<TEntity>(string procName, params KeyValuePair<string, string>[] kvp)
        {
            var parameters = new Dapper.DynamicParameters();
            for (int i = 0; i < kvp.Length; i++)
            {
                parameters.Add(@kvp[i].Key, kvp[i].Value);
            }

            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.QueryStoredProc(procName, parameters);
        }

        public static TEntity GetById<TEntity>(object id)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetById(id);
        }

        public static int? Add<TEntity>(TEntity entityToAdd)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.Add(entityToAdd);
        }

        public static void Update<TEntity>(TEntity entityToUpdate)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Update(entityToUpdate);
        }

        public static void Delete<TEntity>(object id)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Delete(id);
        }

        public static void Delete<TEntity>(TEntity entityToDelete)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Delete(entityToDelete);
        }

        public static IEnumerable<IFileItem> GetAllFileLogs()
        {
            var repo = new BaseRepository<FileLog>();
            return repo.GetAll();
        }

        public static IEnumerable<IFileItem> GetAllFileLogs(int integrationID)
        {
            string procName = "GetFileLogsByIntegration";
            var param = new Dapper.DynamicParameters();
            param.Add(@"integrationID", integrationID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);

            var repo = new BaseRepository<FileLog>();
            return repo.QueryStoredProc(procName, param);
        }

        public static IEnumerable<IFileItem> GetFileLogsByFileGuids(IEnumerable<string> fileGuids)
        {
            string procName = "GetFileLogsByFileGuids";
            var param = new Dapper.DynamicParameters();
            param.Add(@"fileGuids", string.Join(",", fileGuids), System.Data.DbType.String, System.Data.ParameterDirection.Input);

            var repo = new BaseRepository<FileLog>();
            return repo.QueryStoredProc(procName, param);
        }

        public static IEnumerable<IFileItem> GetAllFileLogsBySource(int sourceID)
        {
            var repo = new BaseRepository<FileLog>();
            return repo.GetItems(new { SourceID = sourceID });
        }

        public static IEnumerable<IFileItem> GetActiveAPIEntityFileLogs(int sourceID, int integrationID, string entityID, DateTime startDate, bool shouldReturnIsDimOnly = false)
        {
            string procName = "GetActiveAPIEntitiesFileLogs";
            var param = new Dapper.DynamicParameters();
            param.Add(@"sourceID", sourceID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
            param.Add(@"integrationID", integrationID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
            param.Add(@"EntityID", entityID, System.Data.DbType.String, System.Data.ParameterDirection.Input);
            param.Add(@"StartDate", startDate, System.Data.DbType.DateTime, System.Data.ParameterDirection.Input);
            param.Add(@"@IncludeIsDimOnly", shouldReturnIsDimOnly, System.Data.DbType.Boolean, System.Data.ParameterDirection.Input);

            var repo = new BaseRepository<FileLog>();
            return repo.QueryStoredProc(procName, param);
        }

        public static IEnumerable<IFileItem> GetDistinctFileLogDateAndHour(int integrationID)
        {
            var procName = "GetDistinctFileDateAndHour";
            var param = new Dapper.DynamicParameters();
            param.Add("@integrationID", integrationID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
            var repo = new BaseRepository<FileLog>();
            return repo.QueryStoredProc(procName, param);
        }

        public static IEnumerable<IFileItem> GetLatestDeliveryFileDate(int integrationID, DateTime startDate)
        {
            var param = new Dapper.DynamicParameters();
            param.Add("@integrationID", integrationID, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
            param.Add("@StartDate", startDate, System.Data.DbType.DateTime, System.Data.ParameterDirection.Input);
            var repo = new BaseRepository<FileLog>();
            return repo.QueryStoredProc(SP_NAME_GET_LATEST_DELIVERY_FILEDATE, param);
        }

        public static DateTime? GetMaxFileDateProcessedComplete(int integrationID, string entityID)
        {
            var queueRepo = new QueueRepository();
            return queueRepo.GetMaxFileDateProcessedComplete(integrationID, entityID);
        }

        #region JobLog

        public static dynamic GetAllJobLogs()
        {
            return new BaseRepository<dynamic>().QueryStoredProc("GetJobLogs");
        }

        public static JobLog GetJobLogById(object id)
        {
            var repo = new BaseRepository<JobLog>();
            return repo.GetById(id);
        }

        public static int? AddJobLog(JobLog entity)
        {
            var repo = new BaseRepository<JobLog>();
            return repo.Add(entity);
        }

        public static void DeleteJobLog(object id)
        {
            var repo = new BaseRepository<JobLog>();
            repo.Delete(id);
        }

        public static void DeleteJobLog(JobLog entityToDelete)
        {
            var repo = new BaseRepository<JobLog>();
            repo.Delete(entityToDelete);
        }

        public static void UpdateJobLog(JobLog entityToUpdate)
        {
            var repo = new BaseRepository<JobLog>();
            repo.Update(entityToUpdate);
        }

        public static Int64? SaveJobLog(JobLog entity)
        {
            var repo = new BaseRepository<JobLog>();
            if (entity.JobLogID > 0)
            {
                repo.Update(entity);
                return entity.JobLogID;
            }
            else
            {
                return repo.Add(entity);
            }
        }

        public static Int64? SaveJobLogAsync(JobLog entity)
        {
            var repo = new BaseRepository<JobLog>();
            if (entity.JobLogID > 0)
            {
                repo.UpdateAsync(entity);
                return entity.JobLogID;
            }
            else
            {
                return repo.AddAsync(entity);
            }
        }

        #endregion

        public static IEnumerable<IFileItem> GetAllQueueItemsBySource(int sourceID, bool shouldIncludeIsDimOnly = false)
        {
            var repo = new BaseRepository<Queue>();

            if (shouldIncludeIsDimOnly)
                return repo.GetItems(new { SourceID = sourceID });
            else
                return repo.GetItems(new { SourceID = sourceID, IsDimOnly = false });
        }

        public static IOrderedEnumerable<OrderedQueue> GetTopQueueItemsBySource(int sourceID, int nbResults, long jobLogID, int? integrationID = null)
        {
            var repo = new QueueRepository();
            return repo.GetTopQueueItemsBySource(sourceID, nbResults, jobLogID, integrationID);
        }

        public static IEnumerable<IFileItem> GetActiveOrderedTopQueueItemsBySource(int sourceID, int nbResults, long jobLogID, int? integrationID = null)
        {
            var repo = new QueueRepository();
            return repo.GetOrderedTopQueueItemsBySource(sourceID, nbResults, jobLogID, integrationID);
        }

        public static IEnumerable<IFileItem> GetQueueProcessing(long integrationID, long jobLogID, bool isBackfill = false)
        {
            var repo = new QueueRepository();
            return repo.GetQueueProcessing(integrationID, jobLogID, isBackfill);
        }

        public static IOrderedEnumerable<OrderedQueue> GetOrderedQueueProcessing(long integrationID, long jobLogID, bool isBackfill = false)
        {
            var repo = new QueueRepository();
            return repo.GetOrderedQueueProcessing(integrationID, jobLogID, isBackfill);
        }

        public static IOrderedEnumerable<OrderedQueue> GetOrderedQueueProcessingBySource(long sourceID, long jobLogID, bool isBackfill = false, int nbResults = 200)
        {
            var repo = new QueueRepository();
            return repo.GetOrderedQueueProcessingBySource(sourceID, jobLogID, isBackfill, nbResults);
        }

        public static IFileItem GetByFileGUID(Guid guid)
        {
            var repo = new QueueRepository();
            return repo.GetByFileGUID(guid);
        }

        /// <summary>
        /// Get the number of queues per Integration for the source provided
        /// </summary>
        public static Dictionary<int, int> GetQueueCountByIntegrationID(int sourceId)
        {
            var repo = new QueueRepository();
            return repo.GetQueueCountByIntegrationID(sourceId);
        }

        public static IEnumerable<long> GetQueueIDBySource(int sourceID, int? integrationID = null)
        {
            var repo = new QueueRepository();
            return repo.GetQueueIDBySource(sourceID, integrationID);
        }

        public static IEnumerable<Guid> GetQueueGuidBySource(int sourceID)
        {
            var repo = new QueueRepository();
            return repo.GetQueueGuidBySource(sourceID);
        }

        public static IEnumerable<Guid> GetQueueGuidByIntegration(int integrationID)
        {
            var repo = new QueueRepository();
            return repo.GetQueueGuidByIntegration(integrationID);
        }

        public static IEnumerable<IFileItem> GetQueueItemsByIdList(List<long> queueIdList, int sourceId, long jobLogID)
        {
            var repo = new QueueRepository();
            return repo.GetQueueItemsByIdList(queueIdList, sourceId, jobLogID);
        }

        /// <summary>
        /// Updates status of queue record.
        /// Log-level sources by default have all queues reset to Import-Complete on Error.
        /// Set "updateBatchOnError" to false if queueID should be the only record updated.
        /// </summary>
        public static void UpdateQueueStatus(long queueID, Common.Constants.JobStatus jobStatus, bool updateBatchOnError = true)
        {
            var repo = new QueueRepository();
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@queueID", queueID);
            parameters.Add("@status", (int)jobStatus);
            parameters.Add("@updateBatchOnError", updateBatchOnError);
            repo.ExecuteStoredProc("UpdateQueue", parameters);
        }

        public static void UpdateQueueProcessing(long IntegrationID, string entityID, DateTime fileDate, long jobLogID)
        {
            var repo = new QueueRepository();
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@IntegrationID", IntegrationID);
            parameters.Add("@entityID", entityID);
            parameters.Add("@fileDate", fileDate);
            parameters.Add("@JobLogID", jobLogID);
            repo.ExecuteStoredProc("UpdateQueueProcessing", parameters);
        }

        public static void DeleteQueue(long queueID)
        {
            var repo = new QueueRepository();
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@queueID", queueID);
            repo.ExecuteStoredProc("DeleteQueue", parameters);
        }

        public static bool IsBackfillQueue(int integrationId, int offsetHours)
        {
            var repo = new QueueRepository();
            return repo.IsBackfillQueue(integrationId, offsetHours);
        }

        public static IEnumerable<int> GetActiveQueueIntegrations()
        {
            var repo = new QueueRepository();
            return repo.GetActiveQueueIntegrations();
        }

        #region [Job Scheduler]
        public static IEnumerable<SchedulerConfiguration> GetSchedulerConfigByJobType(int jobTypeId)
        {
            var scheduerConfigRepo = new SchedulerConfigurationRepository();
            return scheduerConfigRepo.GetSchedulerConfigByJobType(jobTypeId);
        }
        #endregion

        #region API

        public static IEnumerable<Greenhouse.Data.Model.Aggregate.APIEntity> GetAllActiveAPIEntities(int sourceId, long? integrationID = null)
        {
            var repo = new BaseRepository<APIEntity>();
            if (integrationID != null)
                return repo.GetItems(new { SourceID = sourceId, IsActive = true, IntegrationID = integrationID });
            return repo.GetItems(new { SourceID = sourceId, IsActive = true });
        }

        public static IEnumerable<Greenhouse.Data.Model.Aggregate.APIEntity> GetAllActiveAPIEntities()
        {
            var repo = new BaseRepository<APIEntity>();
            return repo.GetItems(new { IsActive = true });
        }
        public static IEnumerable<APIEntity> GetAllAPIEntities(int sourceId, long integrationID)
        {
            var repo = new BaseRepository<APIEntity>();
            return repo.GetItems(new { SourceID = sourceId, IntegrationID = integrationID });
        }
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="T">Type of the APIReport Settings</typeparam>
        /// <param name="sourceId"></param>
        /// <returns></returns>
        public static IEnumerable<Generic.APIReport<T>> GetAllActiveAPIReports<T>(int sourceId)
        {
            var repo = new BaseRepository<Generic.APIReport<T>>();
            var fieldRepo = new BaseRepository<APIReportField>();

            var reports = repo.GetItems(new { SourceID = sourceId, IsActive = true });
            foreach (Generic.APIReport<T> rpt in reports)
            {
                rpt.ReportFields = fieldRepo.GetItems(new { APIReportID = rpt.APIReportID, IsActive = true });
            }
            return reports;
        }

        public static void DeletePartitionByFileGUID(string fileguid)
        {
            var repo = new QueueRepository();
            var procName = "DeletePartitionByFileGUID";
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@fileguid", fileguid);
            repo.ExecuteStoredProc(procName, parameters);
        }
        #endregion

        public static IEnumerable<QueueLog> GetAllQueueLogs(DateTime startDate)
        {
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@StartDate", startDate);
            var repo = new BaseRepository<QueueLog>();
            return repo.QueryStoredProc("GetQueueLogs", parameters);
        }

        public static void RemoveDatabricksFailedJobQueue(long queueID)
        {
            var baseRepository = new BaseRepository<DatabricksFailedJobs>();

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@QueueID", queueID);

            baseRepository.QueryStoredProc("RemoveDatabricksFailedJob", parameters);
        }

        public static void InsertDatabricksFailedJobQueue(long queueID, long runID, string status, long jobID, string jobParameters = null)
        {
            var baseRepository = new BaseRepository<DatabricksFailedJobs>();

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@QueueID", queueID);
            parameters.Add("@RunID", runID);
            parameters.Add("@Status", status);
            parameters.Add("@DatabricksJobID", jobID);
            parameters.Add("@DatabricksJobParameters", jobParameters);

            baseRepository.QueryStoredProc("InsertOrUpdateFailedDatabricksJobRun", parameters);
        }
        public static IEnumerable<DatabricksFailedJobs> GetAllDatabricksFailedJobs(List<OrderedQueue> queues)
        {
            var baseRepository = new BaseRepository<DatabricksFailedJobs>();
            var allJobs = baseRepository.GetAll();
            var queueIds = queues.Select(q => q.ID).ToList();
            return allJobs.Where(job => queueIds.Contains(job.QueueID));
        }
    }
}

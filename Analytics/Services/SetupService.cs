using Dapper;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using System;
using System.Collections.Generic;

namespace Greenhouse.Data.Services
{
    public static class SetupService
    {
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

        public static IEnumerable<TEntity> GetItems<TEntity>(object whereClause)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetItems(whereClause);
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

        public static IEnumerable<TEntity> GetQuartzTriggers<TEntity>()
        {
            var baseRepository = new QuartzBaseRepository<TEntity>();
            return baseRepository.QueryStoredProc("GetQrtzTriggers");
        }

        public static int DeleteJob(string jobName, string jobGroup)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@JOB_NAME", jobName);
            parameters.Add("@JOB_GROUP", jobGroup);
            var baseRepository = new QuartzBaseRepository<int>();
            return baseRepository.ExecuteStoredProc("DeleteTriggerAndJob", parameters);
        }

        public static int UpdateJobNextFireTime(string TriggerName, Int64 NextFireTime)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@TriggerName", TriggerName);
            parameters.Add("@NextFireTime", NextFireTime);
            var baseRepository = new QuartzBaseRepository<int>();
            return baseRepository.ExecuteStoredProc("UpdateJobNextFireTime", parameters);
        }

        public static IEnumerable<TEntity> CheckJobExists<TEntity>(string Description, string JobGroup)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@Description", Description);
            parameters.Add("@JobGroup", JobGroup);
            var baseRepository = new QuartzBaseRepository<TEntity>();
            var quartzConnectionstring = Greenhouse.Configuration.Settings.Current.Quartz.ConnectionString;
            return baseRepository.QueryStoredProc("CheckJobExists", parameters);
        }

        public static IEnumerable<Cluster> GetClustersByEnv(string env)
        {
            var cRepo = new ClusterRepository();
            return cRepo.GetClustersByEnv(env);
        }

        public static IEnumerable<Server> GetServersByEnv(string env)
        {
            var sRepo = new ServerRepository();
            return sRepo.GetServersByEnv(env);
        }

        public static IEnumerable<Server> GetServers(int executionTypeID)
        {
            var sRepo = new ServerRepository();
            return sRepo.GetServers(executionTypeID);
        }

        public static IEnumerable<Source> GetSources(int jobStepId)
        {
            var sRepo = new SourceRepository();
            return sRepo.GetSources(jobStepId);
        }

        public static IEnumerable<SourceJob> GetMappedSourceJobs(int? sourceId, int executionTypeID)
        {
            var sRepo = new SourceJobRepository();
            return sRepo.GetMappedSourceJobs(sourceId, executionTypeID);
        }

        public static IEnumerable<SourceJob> GetSourceJobs(int sourceID, int sourceJobStepID)
        {
            var sRepo = new SourceJobRepository();
            return sRepo.GetSourceJobs(sourceID, sourceJobStepID);
        }

        public static IEnumerable<SourceJobStep> sourcejobstepTypes(int jobTypeID)
        {
            var sRepo = new SourceJobStepRepository();
            return sRepo.GetSourceJobStepTypes(jobTypeID);
        }

        public static IEnumerable<JobStatusSourceFile> GetJobStatusSourceFile()
        {
            var baseRepository = new BaseRepository<JobStatusSourceFile>();

            // "UpdateJobStatus" NOT ONLY INSERTS INTO THE "JobStatusSourceFile" TABLE
            //  BUT ALSO RETURNS Delayed AND Error RECORDS
            return baseRepository.QueryStoredProc("UpdateJobStatus");
        }

        public static int InsertIntoLookup(string key, string value, bool isEditable = false)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@Name", key);
            parameters.Add("@Value", value);
            parameters.Add("@IsEditable", isEditable);
            var baseRepository = new BaseRepository<Lookup>();

            return baseRepository.ExecuteStoredProc("InsertLookup", parameters);
        }

        public static int UpdateDataStatusIntegration()
        {
            var baseRepository = new BaseRepository<DataStatusIntegration>();
            return baseRepository.ExecuteStoredProc("UpdateDataStatusIntegration");
        }

        public static int UpdateDataStatusSource()
        {
            var baseRepository = new BaseRepository<DataStatusSource>();
            return baseRepository.ExecuteStoredProc("UpdateDataStatusSource");
        }

        public static int UpdateEventLevelDataStatus()
        {
            var baseRepository = new BaseRepository<EventLevelDataStatus>();
            return baseRepository.ExecuteStoredProc("UpdateEventLevelDataStatus");
        }

        public static IEnumerable<MetadataPartition> GetAgencyMetastoreStatus()
        {
            var baseRepository = new MetadataPartitionRepository();
            return baseRepository.GetAgencyMetastoreStatus();
        }

        public static IEnumerable<System.Reflection.PropertyInfo> GetPropertyInfo<TEntity>()
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetPropertyInfo();
        }
    }
}

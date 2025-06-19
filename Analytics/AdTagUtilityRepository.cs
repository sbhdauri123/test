using Greenhouse.Data.Repositories;
using NLog;
using System;

namespace Greenhouse.Data
{
    public static class AdTagUtilitiesRepository
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        /*DELETE OLD JOB RUN ETNRIES*/
        public static void CleanupJobRun(string noOfDays)
        {
            try
            {
                var parameters = new Dapper.DynamicParameters();
                parameters.Add("NoOfDays", noOfDays, System.Data.DbType.Int32, System.Data.ParameterDirection.Input);
                var baseRepository = new AdTagBaseRepository<string>();
                baseRepository.QueryStoredProc("CleanUpJobRun", parameters);
            }
            catch (Exception ex)
            {
                logger.Log(NLog.LogLevel.Error, string.Format("ERROR: {0}\nStack Trace:\n{1}", ex.Message, ex.StackTrace));
                throw;
            }
        }
    }
}

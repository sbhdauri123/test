using Greenhouse.Data.Model.AdTag.APIAdServer;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public static class APIAdServerRequestRepository
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        /*GET ALL ACTIVE ADVERTISERS*/
        public static List<APIAdServerRequestMapping> GetAllAPIAdServerRequestMappings()
        {
            try
            {
                var adTagBaseRepository = new AdTagBaseRepository<APIAdServerRequestMapping>();
                return adTagBaseRepository.QueryStoredProc("GetActiveAPIAdServerRequests").ToList();
            }
            catch (Exception ex)
            {
                logger.Log(NLog.LogLevel.Error, string.Format("ERROR: {0}\nStack Trace:\n{1}", ex.Message, ex.StackTrace));
                throw;
            }
        }
    }
}
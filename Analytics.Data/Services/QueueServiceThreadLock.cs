using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Repositories;
using System.Collections.Generic;

namespace Greenhouse.Data.Services
{
    public class QueueServiceThreadLock
    {
        private readonly QueueRepository _queueRepo = new QueueRepository();
        private static object _lockObject;

        public QueueServiceThreadLock(object lockObject)
        {
            _lockObject = lockObject;
        }

        public IEnumerable<IFileItem> GetOrderedTopQueueItemsByCredential(int sourceID, int nbResults, long jobLogID, int credentialID, int parentIntegrationID)
        {
            lock (_lockObject)
            {
                return _queueRepo.GetOrderedTopQueueItemsByCredential(sourceID, nbResults, jobLogID, credentialID, parentIntegrationID);
            }
        }
    }
}

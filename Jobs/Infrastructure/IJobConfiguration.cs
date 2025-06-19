using Greenhouse.Data.Model.Core;

namespace Greenhouse.Jobs.Infrastructure
{
    public interface IJobConfiguration
    {
        bool CanExecute(JobExecutionDetails jed);
        int GetAutoRetryCount(string key);
        bool IsTriggerExecuting(string triggerName);
        Greenhouse.Caching.ICacheStore TransientCache { get; }
        void PurgePersistentCache();
    }
}

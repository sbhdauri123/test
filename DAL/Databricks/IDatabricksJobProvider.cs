using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.DAL.Databricks
{
    public interface IDatabricksJobProvider
    {
        public List<DatabricksJobResult> RunningJobs { get; }
        public int MaxConcurrentJobs { get; }
        Dictionary<string, string> CreateStandardizedJobParameters(DatabricksJobParameterOptions options);
        Task QueueJobAsync(long queueID, JobRunRequest jobRequest, Action<long, long> onException, CancellationToken cancellationToken);
        Task WaitForMaxRunJobsToCompleteAsync(Action<DatabricksJobResult> onJobCompletion, Action<long, long> onException, CancellationToken cancellationToken, bool checkAllJobsNow = false);
        Task WaitForJobToCompleteAsync(long queueID, Action<DatabricksJobResult> onJobCompletion, Action<long> onException, CancellationToken cancellationToken);
        void InitializeRunningJobs(List<long> queueIDs);
    }
}

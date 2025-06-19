using Greenhouse.Data.Model.Core;
using System.Collections.Generic;

namespace Greenhouse.Jobs.Infrastructure
{
    public interface IJobSchedulerHelper
    {
        void ScheduleBatchChainedJobs(JobExecutionDetails JED, List<JobExecutionDetails> chainedJEDs);

        void ScheduleDynamicJob(JobExecutionDetails newJED);

        void RetryJob(JobExecutionDetails JED, string qualifiedName);
    }
}

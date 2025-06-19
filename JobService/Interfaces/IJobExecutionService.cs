using Greenhouse.Contracts.Messages;

namespace Greenhouse.JobService.Interfaces;

public interface IJobExecutionService
{
    Task ExecuteJobAsync(ExecuteJob executeJob, CancellationToken cancellationToken = default);
}
using Greenhouse.Contracts.Messages;

namespace Greenhouse.JobService.Interfaces;

public interface IJobStrategy
{
    string ContractKey { get; }
    Task ExecuteAsync(ExecuteJob executeJob, CancellationToken cancellationToken = default);
}
using Greenhouse.Common.Extensions;
using Greenhouse.Contracts.Messages;
using Greenhouse.JobService.Interfaces;

namespace Greenhouse.JobService.Services;

public class JobExecutionService : IJobExecutionService
{
    private readonly IJobStrategyFactory _jobStrategyFactory;
    private readonly ILogger<JobExecutionService> _logger;

    public JobExecutionService(IJobStrategyFactory jobStrategyFactory, ILogger<JobExecutionService> logger)
    {
        ArgumentNullException.ThrowIfNull(jobStrategyFactory);
        ArgumentNullException.ThrowIfNull(logger);

        _jobStrategyFactory = jobStrategyFactory;
        _logger = logger;
    }

    public async Task ExecuteJobAsync(ExecuteJob executeJob, CancellationToken cancellationToken = default)
    {
        IJobStrategy strategy = _jobStrategyFactory.GetStrategy(executeJob.ContractKey);

        _logger.LogMessage(LogLevel.Information, "Executing job strategy for", strategy.ContractKey);

        await strategy.ExecuteAsync(executeJob, cancellationToken);
    }
}
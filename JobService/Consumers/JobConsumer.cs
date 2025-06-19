using Greenhouse.Common.Extensions;
using Greenhouse.Contracts.Messages;
using Greenhouse.JobService.Interfaces;
using MassTransit;

namespace Greenhouse.JobService.Consumers;

public class JobConsumer : IConsumer<ExecuteJob>
{
    private readonly ILogger<JobConsumer> _logger;
    private readonly IJobExecutionService _jobExecutionService;

    public JobConsumer(ILogger<JobConsumer> logger, IJobExecutionService jobExecutionService)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(jobExecutionService);

        _logger = logger;
        _jobExecutionService = jobExecutionService;
    }

    public async Task Consume(ConsumeContext<ExecuteJob> context)
    {
        try
        {
            _logger.LogMessage(LogLevel.Debug, "--> Message received: {Message}", context.Message);

            await _jobExecutionService.ExecuteJobAsync(context.Message);
        }
        catch (Exception e)
        {
            _logger.LogMessage(LogLevel.Error, e.Message, e.StackTrace);
        }
    }
}
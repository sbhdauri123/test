using Greenhouse.Contracts.Messages;
using Greenhouse.JobService.Interfaces;
using Greenhouse.JobService.Services;
using Microsoft.Extensions.Logging;
using NSubstitute.ExceptionExtensions;

namespace Greenhouse.JobService.Tests.Unit.Services;

public class JobExecutionServiceTests
{
    private readonly ILogger<JobExecutionService> _logger;
    private readonly IJobStrategyFactory _jobStrategyFactory;
    private readonly JobExecutionService _sut;

    public JobExecutionServiceTests()
    {
        _logger = Substitute.For<ILogger<JobExecutionService>>();
        _jobStrategyFactory = Substitute.For<IJobStrategyFactory>();
        _sut = new JobExecutionService(_jobStrategyFactory, _logger);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        Func<JobExecutionService> act = () => new JobExecutionService(_jobStrategyFactory, _logger);
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        Func<JobExecutionService> act = () => new JobExecutionService(_jobStrategyFactory, null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    [Fact]
    public void Constructor_WithNullJobStrategyFactory_ThrowsArgumentNullException()
    {
        Func<JobExecutionService> act = () => new JobExecutionService(null!, _logger);
        act.Should().Throw<ArgumentNullException>().WithParameterName("jobStrategyFactory");
    }

    #endregion

    [Fact]
    public async Task ExecuteJobAsync_WhenCalledWithValidJob_ShouldExecuteCorrectStrategy()
    {
        // Arrange
        ExecuteJob executeJob = new()
        {
            ContractKey = "TestContract",
            JobGuid = default,
            Step = Common.Constants.JobStep.Start,
            SourceId = 0,
            IntegrationId = 0,
            ServerId = 0
        };
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        strategy.ContractKey.Returns("TestContract");
        _jobStrategyFactory.GetStrategy(executeJob.ContractKey).Returns(strategy);

        // Act
        await _sut.ExecuteJobAsync(executeJob);

        // Assert
        await strategy.Received(1).ExecuteAsync(executeJob, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteJobAsync_WhenStrategyExecutionFails_ShouldPropagateException()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob("TestContract");
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        InvalidOperationException expectedException = new("Strategy execution failed");

        strategy.ContractKey.Returns("TestContract");
        strategy.ExecuteAsync(Arg.Any<ExecuteJob>(), Arg.Any<CancellationToken>())
            .Throws(expectedException);

        _jobStrategyFactory.GetStrategy(executeJob.ContractKey).Returns(strategy);

        // Act
        Func<Task> act = () => _sut.ExecuteJobAsync(executeJob);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Strategy execution failed");
    }

    [Fact]
    public async Task ExecuteJobAsync_WhenCancellationRequested_ShouldRespectCancellation()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob("TestContract");
        IJobStrategy? strategy = Substitute.For<IJobStrategy>();
        strategy.ContractKey.Returns("TestContract");
        strategy.ExecuteAsync(Arg.Any<ExecuteJob>(), Arg.Any<CancellationToken>())
            .Returns(async caller =>
            {
                caller.Arg<CancellationToken>().ThrowIfCancellationRequested();
                await Task.CompletedTask;
            });
        _jobStrategyFactory.GetStrategy(executeJob.ContractKey).Returns(strategy);

        CancellationTokenSource cts = new();
        await cts.CancelAsync();

        // Act
        Func<Task> act = () => _sut.ExecuteJobAsync(executeJob, cts.Token);

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #region Helpers

    private static ExecuteJob CreateExecuteJob(string contractKey)
    {
        ExecuteJob executeJob = new()
        {
            ContractKey = contractKey,
            JobGuid = default,
            Step = Common.Constants.JobStep.Start,
            SourceId = 0,
            IntegrationId = 0,
            ServerId = 0
        };
        return executeJob;
    }

    #endregion
}
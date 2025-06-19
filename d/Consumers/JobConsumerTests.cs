using Greenhouse.Common.Exceptions;
using Greenhouse.Contracts.Messages;
using Greenhouse.JobService.Consumers;
using Greenhouse.JobService.Interfaces;
using MassTransit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using NSubstitute.ExceptionExtensions;

namespace Greenhouse.JobService.Tests.Unit.Consumers;

public class JobConsumerTests
{
    private readonly IJobExecutionService _jobExecutionService;
    private readonly ConsumeContext<ExecuteJob> _context;
    private readonly FakeLogger<JobConsumer> _logger;
    private readonly JobConsumer _sut;

    public JobConsumerTests()
    {
        _logger = new FakeLogger<JobConsumer>();
        _jobExecutionService = Substitute.For<IJobExecutionService>();
        _context = Substitute.For<ConsumeContext<ExecuteJob>>();
        _sut = new JobConsumer(_logger, _jobExecutionService);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        Func<JobConsumer> act = () => new JobConsumer(_logger, _jobExecutionService);
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        Func<JobConsumer> act = () => new JobConsumer(null!, _jobExecutionService);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    [Fact]
    public void Constructor_WithNullJobExecutionService_ThrowsArgumentNullException()
    {
        Func<JobConsumer> act = () => new JobConsumer(_logger, null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("jobExecutionService");
    }

    #endregion

    [Fact]
    public async Task Consume_WhenMessageIsValid_ShouldExecuteJobAndLogMessage()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob();
        _context.Message.Returns(executeJob);

        // Act
        await _sut.Consume(_context);

        // Assert
        await _jobExecutionService.Received(1).ExecuteJobAsync(executeJob);
        _logger.Collector.Count.Should().Be(1);
        _logger.Collector.LatestRecord.Level.Should().Be(LogLevel.Debug);
        _logger.Collector.LatestRecord.Message.Should().Contain("--> Message received:");
    }

    [Fact]
    public async Task Consume_WhenExecutionFails_ShouldLogError()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob();
        InvalidOperationException expectedException = new("Test exception");
        _context.Message.Returns(executeJob);

        _jobExecutionService
            .ExecuteJobAsync(Arg.Any<ExecuteJob>())
            .Throws(expectedException);

        // Act
        await _sut.Consume(_context);

        // Assert
        _logger.Collector.Count.Should().Be(2);
        _logger.Collector.LatestRecord.Level.Should().Be(LogLevel.Error);
        _logger.Collector.LatestRecord.Message.Should().Contain(expectedException.Message);
    }

    [Fact]
    public async Task Consume_WhenExecutionSucceeds_ShouldNotLogErrors()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob();
        _context.Message.Returns(executeJob);

        // Act
        await _sut.Consume(_context);

        // Assert
        _logger.Collector.Count.Should().Be(1);
        _logger.Collector.LatestRecord.Level.Should().Be(LogLevel.Debug);
        _logger.Collector.LatestRecord.Message.Should().Contain("--> Message received:");
    }

    [Fact]
    public async Task Consume_WhenExceptionOccurs_ShouldContinueExecution()
    {
        // Arrange
        ExecuteJob executeJob = CreateExecuteJob();
        _context.Message.Returns(executeJob);

        _jobExecutionService
            .ExecuteJobAsync(Arg.Any<ExecuteJob>())
            .Throws(new UnitTestException("Test exception"));

        // Act 
        Func<Task> act = () => _sut.Consume(_context);

        // Assert
        await act.Should().NotThrowAsync();
    }

    #region Helpers

    private static ExecuteJob CreateExecuteJob()
    {
        ExecuteJob executeJob = new()
        {
            ContractKey = "TestContract",
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
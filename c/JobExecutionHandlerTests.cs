using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Contracts.Messages;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using MassTransit;
using Microsoft.Extensions.Logging;
using NSubstitute.ExceptionExtensions;

namespace Greenhouse.Jobs.Tests.Unit;

public class JobExecutionHandlerTests
{
    private readonly ILogger<JobExecutionHandler> _logger;
    private readonly IBus _bus;
    private readonly ILookupService _lookupService;
    private readonly JobExecutionHandler _sut;

    public JobExecutionHandlerTests()
    {
        _logger = Substitute.For<ILogger<JobExecutionHandler>>();
        _bus = Substitute.For<IBus>();
        _lookupService = Substitute.For<ILookupService>();
        _sut = new JobExecutionHandler(_logger, _bus, _lookupService);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameter_ShouldNotThrow()
    {
        Func<JobExecutionHandler> act = () => new JobExecutionHandler(_logger, _bus, _lookupService);
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
    {
        Func<JobExecutionHandler> act = () => new JobExecutionHandler(null, _bus, _lookupService);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    [Fact]
    public void Constructor_WithNullBus_ShouldThrowArgumentNullException()
    {
        Func<JobExecutionHandler> act = () => new JobExecutionHandler(_logger, null, _lookupService);
        act.Should().Throw<ArgumentNullException>().WithParameterName("bus");
    }

    [Fact]
    public void Constructor_WithNullLookupService_ShouldThrowArgumentNullException()
    {
        Func<JobExecutionHandler> act = () => new JobExecutionHandler(_logger, _bus, null);
        act.Should().Throw<ArgumentNullException>().WithParameterName("lookupService");
    }

    #endregion

    [Fact]
    public async Task TryPublishJobExecutionMessage_WhenFeatureNotEnabled_ReturnsFalse()
    {
        // Arrange
        ExecuteJob executeJob = CreateValidExecuteJob();
        _lookupService
            .GetAndDeserializeLookupValueWithDefault(
                Arg.Is(Constants.FEATURE_ROUTE_JOBS_TO_MESSAGE_BROKER),
                Arg.Any<List<string>>())
            .Returns([]);

        // Act
        bool result = await _sut.TryPublishJobExecutionMessage(executeJob);

        // Assert
        result.Should().BeFalse();
        await _bus.DidNotReceive().Publish(Arg.Any<ExecuteJob>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TryPublishJobExecutionMessage_WhenFeatureEnabledAndPublishSucceeds_ReturnsTrue()
    {
        // Arrange
        ExecuteJob executeJob = CreateValidExecuteJob();
        SetupLookupServiceForEnabledFeature(executeJob.ContractKey);

        // Act
        bool result = await _sut.TryPublishJobExecutionMessage(executeJob);

        // Assert
        result.Should().BeTrue();

        await _bus.Received(1).Publish(
            Arg.Is<ExecuteJob>(job =>
                job.ContractKey == executeJob.ContractKey &&
                job.JobGuid == executeJob.JobGuid &&
                job.Step == executeJob.Step &&
                job.SourceId == executeJob.SourceId &&
                job.IntegrationId == executeJob.IntegrationId &&
                job.ServerId == executeJob.ServerId &&
                job.TimeZoneString == executeJob.TimeZoneString
            ),
            Arg.Any<CancellationToken>()
        );
    }

    [Fact]
    public async Task TryPublishJobExecutionMessage_WhenBusThrowsNotImplementedException_ReturnsFalse()
    {
        // Arrange
        ExecuteJob executeJob = CreateValidExecuteJob();
        SetupLookupServiceForEnabledFeature(executeJob.ContractKey);
        _bus.Publish(Arg.Any<ExecuteJob>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new NotImplementedException());

        // Act
        bool result = await _sut.TryPublishJobExecutionMessage(executeJob);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public async Task TryPublishJobExecutionMessage_WhenBusThrowsGenericException_ReturnsFalse()
    {
        // Arrange
        ExecuteJob executeJob = CreateValidExecuteJob();
        SetupLookupServiceForEnabledFeature(executeJob.ContractKey);
        UnitTestException testException = new("Test error");
        _bus.Publish(Arg.Any<ExecuteJob>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(testException);

        // Act
        bool result = await _sut.TryPublishJobExecutionMessage(executeJob);

        // Assert
        result.Should().BeFalse();
    }

    #region Helpers

    private static ExecuteJob CreateValidExecuteJob()
    {
        return new ExecuteJob
        {
            ContractKey = "TestContract",
            JobGuid = Guid.NewGuid(),
            Step = Constants.JobStep.Import,
            SourceId = 1,
            IntegrationId = 123,
            ServerId = 2,
            TimeZoneString = "UTC"
        };
    }

    private void SetupLookupServiceForEnabledFeature(string contractKey)
    {
        _lookupService
            .GetAndDeserializeLookupValueWithDefault(
                Arg.Is(Constants.FEATURE_ROUTE_JOBS_TO_MESSAGE_BROKER),
                Arg.Any<List<string>>())
            .Returns([contractKey]);
    }

    #endregion
}
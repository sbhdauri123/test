using Greenhouse.Caching;
using Greenhouse.Contracts.Messages;
using Greenhouse.Jobs.Aggregate.LinkedIn;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.JobService.Constants;
using Greenhouse.JobService.Strategies.LinkedIn;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Microsoft.Extensions.Logging.Testing;

namespace Greenhouse.JobService.Tests.Unit.Strategies.LinkedIn;

public class JobStrategyTests
{
    private readonly FakeLogger<JobStrategy> _logger;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly ITokenCache _tokenCache;
    private readonly JobStrategy _sut;
    private readonly IJobLoggerFactory _jobLoggerFactory;
    private readonly IEnumerable<IDragoJob> _jobs;
    private readonly IJobLogger _jobLogger;
    private readonly ImportJob _importJob;

    public JobStrategyTests()
    {
        _logger = new FakeLogger<JobStrategy>();
        _httpClientProvider = Substitute.For<IHttpClientProvider>();
        _tokenCache = Substitute.For<ITokenCache>();
        _jobLoggerFactory = Substitute.For<IJobLoggerFactory>();
        _jobLogger = Substitute.For<IJobLogger>();
        _importJob = Substitute.For<ImportJob>();

        _jobLoggerFactory.GetJobLogger().Returns(_jobLogger);
        _jobs = new List<IDragoJob> { _importJob };

        _sut = new JobStrategy(_logger, _httpClientProvider, _tokenCache,
            _jobLoggerFactory, _jobs);
    }

    #region Constructor Tests

    [Theory]
    [InlineData("logger")]
    [InlineData("httpClientProvider")]
    [InlineData("tokenCache")]
    [InlineData("jobLoggerFactory")]
    [InlineData("jobs")]
    public void Constructor_WhenDependencyIsNull_ShouldThrowArgumentNullException(string paramName)
    {
        // Arrange
        Dictionary<string, object?> dependencies = new()
        {
            { "logger", _logger },
            { "httpClientProvider", _httpClientProvider },
            { "tokenCache", _tokenCache },
            { "jobLoggerFactory", _jobLoggerFactory },
            { "jobs", _jobs }
        };

        dependencies[paramName] = null;

        // Act
        Func<JobStrategy> act = () => new JobStrategy(
            (FakeLogger<JobStrategy>)dependencies["logger"]!,
            (IHttpClientProvider)dependencies["httpClientProvider"]!,
            (ITokenCache)dependencies["tokenCache"]!,
            (IJobLoggerFactory)dependencies["jobLoggerFactory"]!,
            (IEnumerable<IDragoJob>)dependencies["jobs"]!);

        // Assert
        act.Should()
            .Throw<ArgumentNullException>()
            .WithParameterName(paramName);
    }

    #endregion

    [Fact]
    public void ContractKey_ShouldReturnCorrectValue()
    {
        _sut.ContractKey.Should().Be(ContractKeys.LinkedInAggregateImportJob);
    }

    [Fact]
    public async Task ExecuteAsync_WhenJobNotFound_ShouldThrowInvalidOperationException()
    {
        // Arrange
        IDragoJob? nonImportJob = Substitute.For<IDragoJob>();
        JobStrategy strategy = new(_logger, _httpClientProvider, _tokenCache,
            _jobLoggerFactory, [nonImportJob]);

        ExecuteJob executeJob = CreateExecuteJob();

        // Act
        Func<Task> act = () => strategy.ExecuteAsync(executeJob);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("LinkedIn Import Job not found");
    }

    private static ExecuteJob CreateExecuteJob()
    {
        return new ExecuteJob
        {
            ContractKey = ContractKeys.LinkedInAggregateImportJob,
            JobGuid = default,
            Step = Common.Constants.JobStep.Start,
            SourceId = 0,
            IntegrationId = 0,
            ServerId = 0
        };
    }
}
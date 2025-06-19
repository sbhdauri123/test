using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Repositories;
using Greenhouse.Logging;
using NLog;

namespace Greenhouse.DAL.Tests.Unit;

public class DatabricksJobProviderTests
{
    private readonly IDatabricksJobLogRepository _databricksJobLogRepo;
    private readonly IDatabricksCalls _api;
    private readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private readonly DatabricksJobProvider _databricksJobProvider;
    private readonly DatabricksJobProviderOptions _options;

    public DatabricksJobProviderTests()
    {
        _databricksJobLogRepo = Substitute.For<IDatabricksJobLogRepository>();
        _api = Substitute.For<IDatabricksCalls>();
        _options = new DatabricksJobProviderOptions()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };
        _databricksJobProvider = new DatabricksJobProvider(_options, _api, _databricksJobLogRepo);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(_options, _api, _databricksJobLogRepo);

        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullDatabricksCallsClient_ShouldThrowArgumentNullException()
    {
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(_options, null, _databricksJobLogRepo);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("databricksApiClient");
    }

    [Fact]
    public void Constructor_WithNullDatabricksJobLogRepo_ShouldThrowArgumentNullException()
    {
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(_options, _api, null);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("databricksJobLogRepo");
    }

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(null, _api, _databricksJobLogRepo);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void Constructor_WithNullLoggerOption_ShouldThrowArgumentException()
    {
        // Arrange
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = null,
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        // Act
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(DatabricksJobProviderOptions.Logger));
    }

    [Fact]
    public void Constructor_WithNullExceptionLoggerOption_ShouldThrowArgumentException()
    {
        // Arrange
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = null,
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        // Act
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName(nameof(DatabricksJobProviderOptions.ExceptionLogger));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroMaxConcurrentJobs_ShouldThrowArgumentOutOfRangeException(int maxConcurrentJobs)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = maxConcurrentJobs,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (maxConcurrentJobs <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.MaxConcurrentJobs));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroRetryDelayInSeconds_ShouldThrowArgumentOutOfRangeException(int retryDelayInSeconds)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = retryDelayInSeconds,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (retryDelayInSeconds <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.RetryDelayInSeconds));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroJobRequestRetryMaxAttempts_ShouldThrowArgumentOutOfRangeException(int jobRequestRetryMaxAttempts)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = jobRequestRetryMaxAttempts,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (jobRequestRetryMaxAttempts <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.JobRequestRetryMaxAttempts));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroJobRequestRetryDelayInSeconds_ShouldThrowArgumentOutOfRangeException(int jobRequestRetryDelayInSeconds)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = jobRequestRetryDelayInSeconds,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (jobRequestRetryDelayInSeconds <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.JobRequestRetryDelayInSeconds));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroJobStatusCheckRetryMaxAttempts_ShouldThrowArgumentOutOfRangeException(int jobStatusCheckRetryMaxAttempts)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = jobStatusCheckRetryMaxAttempts,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (jobStatusCheckRetryMaxAttempts <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.JobStatusCheckRetryMaxAttempts));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    public void Constructor_WithNegativeOrZeroJobStatusCheckRetryDelayInSeconds_ShouldThrowArgumentOutOfRangeException(int jobStatusCheckRetryDelayInSeconds)
    {
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = "123",
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = jobStatusCheckRetryDelayInSeconds
        };

        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        if (jobStatusCheckRetryDelayInSeconds <= 0)
        {
            act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatabricksJobProviderOptions.JobStatusCheckRetryDelayInSeconds));
        }
        else
        {
            act.Should().NotThrow();
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WithNullOrWhiteSpaceDatabricksJobID_ShouldThrowArgumentException(string? databricksJobId)
    {
        // Arrange
        DatabricksJobProviderOptions invalidOptions = new()
        {
            IntegrationID = 123,
            JobLogID = -1,
            MaxConcurrentJobs = 1,
            RetryDelayInSeconds = 30,
            DatabricksJobID = databricksJobId,
            Logger = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg))),
            ExceptionLogger = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)),
            JobRequestRetryMaxAttempts = 1,
            JobRequestRetryDelayInSeconds = 30,
            JobStatusCheckRetryMaxAttempts = 1,
            JobStatusCheckRetryDelayInSeconds = 30
        };

        // Act
        Func<DatabricksJobProvider> act = () => new DatabricksJobProvider(invalidOptions, _api, _databricksJobLogRepo);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatabricksJobProviderOptions.DatabricksJobID));
    }

    #endregion

    #region Create Standardized Job Parameters Tests

    [Fact]
    public void CreateStandardizedJobParameters_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Dictionary<string, string>> act = () => _databricksJobProvider.CreateStandardizedJobParameters(null);

        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateStandardizedJobParameters_WithNullOrWhiteSpaceStageFilePath_ShouldThrowArgumentException(string? stageFilePath)
    {
        // Arrange
        DatabricksJobParameterOptions invalidOptions = new()
        {
            StageFilePath = stageFilePath,
            FileGuid = Guid.NewGuid().ToString(),
            FileDate = "01-01-1970",
            EntityID = "123",
        };

        // Act
        Func<Dictionary<string, string>> act = () => _databricksJobProvider.CreateStandardizedJobParameters(invalidOptions);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatabricksJobParameterOptions.StageFilePath));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateStandardizedJobParameters_WithNullOrWhiteFileGuid_ShouldThrowArgumentException(string? fileGuid)
    {
        // Arrange
        DatabricksJobParameterOptions invalidOptions = new()
        {
            StageFilePath = "s3path",
            FileGuid = fileGuid,
            FileDate = "01-01-1970",
            EntityID = "123",
        };

        // Act
        Func<Dictionary<string, string>> act = () => _databricksJobProvider.CreateStandardizedJobParameters(invalidOptions);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatabricksJobParameterOptions.FileGuid));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void CreateStandardizedJobParameters_WithNullOrWhiteSpaceFileDate_ShouldThrowArgumentException(string? fileDate)
    {
        // Arrange
        DatabricksJobParameterOptions invalidOptions = new()
        {
            StageFilePath = "s3path",
            FileGuid = Guid.NewGuid().ToString(),
            FileDate = fileDate,
            EntityID = "123",
        };

        // Act
        Func<Dictionary<string, string>> act = () => _databricksJobProvider.CreateStandardizedJobParameters(invalidOptions);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatabricksJobParameterOptions.FileDate));
    }

    #endregion

    #region Helper methods

    private static string PrefixJobGuid(string message)
    {
        return $"UnitTestMockJobGuid - {message}";
    }

    #endregion
}

using Greenhouse.Auth;
using Greenhouse.DAL.DataSource.Twitter;
using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using ApiClient = Greenhouse.DAL.DataSource.Twitter.ApiClient;
using ApiClientOptions = Greenhouse.DAL.DataSource.Twitter.ApiClientOptions;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.Tests.Unit.DataSource.Twitter;

public class ApiClientTests
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly IOAuthAuthenticator _oAuthAuthenticator;
    private readonly ApiClientOptions _options;
    private readonly ApiClient _apiClient;

    public ApiClientTests()
    {
        _httpClientProvider = Substitute.For<IHttpClientProvider>();
        _oAuthAuthenticator = Substitute.For<IOAuthAuthenticator>();
        _options = new ApiClientOptions { EndpointUri = "https://api.twitter.com", Version = "v11" };
        _apiClient = new ApiClient(_options, _httpClientProvider, _oAuthAuthenticator);
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        // Act
        Func<ApiClient> act = () => new ApiClient(_options, _httpClientProvider, _oAuthAuthenticator);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullHttpClientProvider_ShouldThrowArgumentNullException()
    {
        // Act
        Func<ApiClient> act = () => new ApiClient(_options, null, _oAuthAuthenticator);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("httpClientProvider");
    }

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Act
        Func<ApiClient> act = () => new ApiClient(null, _httpClientProvider, _oAuthAuthenticator);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }

    [Fact]
    public void Constructor_WithNullOAuthAuthenticator_ShouldThrowArgumentNullException()
    {
        // Act
        Func<ApiClient> act = () => new ApiClient(_options, _httpClientProvider, null);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("oAuthAuthenticator");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WithNullOrWhiteSpaceEndpointUri_ShouldThrowArgumentException(string? endpointUri)
    {
        // Arrange
        ApiClientOptions invalidOptions = new() { EndpointUri = endpointUri, Version = "123" };

        // Act
        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _oAuthAuthenticator);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ApiClientOptions.EndpointUri));
    }

    [Fact]
    public void Constructor_WithInvalidEndpointUri_ShouldThrowUriFormatException()
    {
        // Arrange
        ApiClientOptions invalidOptions = new() { EndpointUri = "not a valid uri", Version = "1" };

        // Act
        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _oAuthAuthenticator);

        // Assert
        act.Should().Throw<UriFormatException>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WithNullOrWhiteSpaceVersion_ShouldThrowArgumentException(string? version)
    {
        // Arrange
        ApiClientOptions invalidOptions = new() { EndpointUri = "https://api.twitter.com", Version = version };

        // Act
        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _oAuthAuthenticator);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ApiClientOptions.Version));
    }

    #endregion

    #region GetActiveEntitiesAsync

    [Fact]
    public async Task GetActiveEntitiesAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<ActiveEntities>> act = async () => await _apiClient.GetActiveEntitiesAsync(null);
        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [ClassData(typeof(GetActiveEntitiesOptionsTestData))]
    public async Task GetActiveEntitiesAsync_SendsCorrectRequest(GetActiveEntitiesOptions options, string expectedUrl)
    {
        // Arrange
        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.SendRequestAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(JsonConvert.SerializeObject(new ActiveEntities()));

        // Act
        ActiveEntities? act = await _apiClient.GetActiveEntitiesAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Get);
        capturedRequest?.ContentType.Should().Be("application/json");
        capturedRequest?.Uri.Should().Be(expectedUrl);

        act.Should().NotBeNull();
    }

    #endregion

    #region GetReportRequestStatusAsync

    [Fact]
    public async Task GetReportRequestStatusAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Act
        Func<Task<ReportRequestStatusResponse>> act = async () => await _apiClient.GetReportRequestStatusAsync(null);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task GetReportRequestStatusAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
        string? accountId)
    {
        // Arrange
        GetReportRequestStatusOptions options = new() { AccountId = accountId, JobIds = new List<string> { "abc" } };

        // Act
        Func<Task<ReportRequestStatusResponse>> act = async () => await _apiClient.GetReportRequestStatusAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(GetReportRequestStatusOptions.AccountId));
    }

    [Fact]
    public async Task GetReportRequestStatusAsync_WithNullJobIds_ShouldThrowArgumentNullException()
    {
        // Arrange
        GetReportRequestStatusOptions options = new() { AccountId = "123456", JobIds = null };

        // Act
        Func<Task<ReportRequestStatusResponse>> act = async () => await _apiClient.GetReportRequestStatusAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>()
            .WithParameterName(nameof(GetReportRequestStatusOptions.JobIds));
    }

    [Fact]
    public async Task GetReportRequestStatusAsync_SendsCorrectRequest()
    {
        // Arrange
        GetReportRequestStatusOptions options = new()
        {
            AccountId = "123456",
            JobIds = new List<string> { "abc", "def" }
        };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.SendRequestAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(JsonConvert.SerializeObject(new ReportRequestStatusResponse()));

        // Act
        ReportRequestStatusResponse? act = await _apiClient.GetReportRequestStatusAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Get);
        capturedRequest?.ContentType.Should().Be("application/json");
        capturedRequest?.Uri.Should().Be("https://api.twitter.com/v11/stats/jobs/accounts/123456?job_ids=abc%2Cdef");

        act.Should().NotBeNull();
    }

    #endregion

    #region GetFactReportAsync

    [Fact]
    public async Task GetFactReportAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Act
        Func<Task<ReportRequestResponse>> act = async () => await _apiClient.GetFactReportAsync(null);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Fact]
    public async Task GetFactReportAsync_SendsCorrectRequest()
    {
        // Arrange
        GetFactReportOptions options = new()
        {
            AccountId = "123",
            Entity = "tweet",
            Granularity = "DAY",
            Placement = "ALL_ON_TWITTER",
            MetricGroups = "ENGAGEMENT",
            ReportType = "ENTITY",
            FileDate = new DateTime(2024, 7, 31),
            EntityIds = new List<string> { "456" }
        };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.SendRequestAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(JsonConvert.SerializeObject(new ReportRequestResponse()));

        // Act
        ReportRequestResponse? act = await _apiClient.GetFactReportAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.Uri.Should()
            .Be(
                "https://api.twitter.com/v11/ENTITY/jobs/accounts/123?end_time=2024-08-01&entity=tweet&entity_ids=456&granularity=DAY&metric_groups=ENGAGEMENT&placement=ALL_ON_TWITTER&start_time=2024-07-31");

        act.Should().NotBeNull();
    }

    #endregion

    #region DownloadDimensionFileAsync

    [Fact]
    public async Task DownloadDimensionFileAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadDimensionFileAsync(null);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null,
        "https://api.twitter.com/v11/accounts?count=10&entityIdParamName1=entity1&include_legacy_cards=true&timeline_type=ALL&tweet_type=PUBLISHED%20&with_deleted=true")]
    [InlineData("",
        "https://api.twitter.com/v11/accounts?count=10&entityIdParamName1=entity1&include_legacy_cards=true&timeline_type=ALL&tweet_type=PUBLISHED%20&with_deleted=true")]
    [InlineData("endpoint1",
        "https://api.twitter.com/v11/accounts/123/endpoint1?count=10&entityIdParamName1=entity1&include_legacy_cards=true&timeline_type=ALL&tweet_type=PUBLISHED%20&with_deleted=true")]
    public async Task DownloadDimensionFileAsync_SendsCorrectRequest(string? endpoint, string expectedUri)
    {
        // Arrange
        ReportSettings reportSettings = new()
        {
            ReportType = "reportType1",
            Entity = "entity1",
            Granularity = "HOUR",
            Endpoint = endpoint,
            EntityIdsParamName = "entityIdParamName1",
            TweetType = "PUBLISHED ",
            WithDeleted = true,
            IncludeLegacyCards = true,
            PageSize = "10",
            TimelineType = "ALL"
        };

        APIReport<ReportSettings> report = new() { ReportSettingsJSON = JsonConvert.SerializeObject(reportSettings) };

        DownloadDimensionFileOptions options = new()
        {
            AccountId = "123",
            EntityIds = new List<string> { "entity1" },
            Report = report
        };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadDimensionFileAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Get);
        capturedRequest?.ContentType.Should().Be("application/json");
        capturedRequest?.Uri.Should().Be(expectedUri);
    }

    #endregion

    #region DownloadReportFileAsync

    [Fact]
    public async Task DownloadReportFileAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadReportFileAsync(null);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Fact]
    public async Task DownloadReportFileAsync_SendsCorrectRequest()
    {
        // Arrange
        const string expectedReportUrl = "https://api.twitter.com/2/reports/123.gz";
        DownloadReportOptions options = new() { AccountId = "123", ReportUrl = expectedReportUrl };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.BuildRequestMessage(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new HttpRequestMessage());

        _httpClientProvider.DownloadFileStreamAsync(Arg.Any<HttpRequestMessage>())
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadReportFileAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Get);
        capturedRequest?.ContentType.Should().Be("application/json");
        capturedRequest?.Uri.Should().Be(expectedReportUrl);
    }

    #endregion
}

#region Theory Test Data

public sealed class GetActiveEntitiesOptionsTestData : TheoryData<GetActiveEntitiesOptions, string>
{
    private const string ExpectedUrlForDayGranularity =
        "https://api.twitter.com/v11/stats/accounts/123456/active_entities?end_time=2024-08-01&entity=abc&start_time=2024-07-31";

    private const string ExpectedUrlForAnyOtherGranularity =
        "https://api.twitter.com/v11/stats/accounts/123456/active_entities?end_time=2024-08-01T00%3A00%3A00Z&entity=abc&start_time=2024-07-31T00%3A00%3A00Z";

    public GetActiveEntitiesOptionsTestData()
    {
        Add(new GetActiveEntitiesOptions
        {
            AccountId = "123456",
            Entity = "abc",
            FileDate = new DateTime(2024, 7, 31),
            Granularity = "DAY",
        },
            ExpectedUrlForDayGranularity);

        Add(new GetActiveEntitiesOptions
        {
            AccountId = "123456",
            Entity = "abc",
            FileDate = new DateTime(2024, 7, 31),
            Granularity = null,
        },
            ExpectedUrlForAnyOtherGranularity);

        Add(new GetActiveEntitiesOptions
        {
            AccountId = "123456",
            Entity = "abc",
            FileDate = new DateTime(2024, 7, 31),
            Granularity = "",
        },
            ExpectedUrlForAnyOtherGranularity);

        Add(new GetActiveEntitiesOptions
        {
            AccountId = "123456",
            Entity = "abc",
            FileDate = new DateTime(2024, 7, 31),
            Granularity = "   ",
        },
            ExpectedUrlForAnyOtherGranularity);

        Add(new GetActiveEntitiesOptions
        {
            AccountId = "123456",
            Entity = "abc",
            FileDate = new DateTime(2024, 7, 31),
            Granularity = "HOUR",
        },
            ExpectedUrlForAnyOtherGranularity);
    }
}

#endregion
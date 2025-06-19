using Greenhouse.DAL.DataSource.AmazonAdsApi;
using Greenhouse.Data.DataSource.AmazonAdsApi;
using Greenhouse.Data.DataSource.AmazonAdsApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using NSubstitute.ExceptionExtensions;
using System.Dynamic;
using System.Text;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.Tests.Unit.DataSource.AmazonAdsApi;

public class AmazonAdsApiServiceTests
{
    private readonly Action<LogLevel, string>? _logMessage;
    private readonly Action<LogLevel, string, Exception>? _logException;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly AmazonAdsApiService _amazonAdsApiService;
    private AmazonAdsApiServiceArguments serviceArguments;

    private string _clientId = "testClientId";
    private string _clientSecret = "testClientSecret";
    private string _refreshToken = "testRefreshToken";
    private string _authorizationCode = "testAuthorizationCode";
    private string _redirectUri = "https://amazon.com/ap/oa";
    private string _endpointUrl = "https://api.amazon.com/auth/o2/token";
    private string _profileUrl = "https://api.amazon.com/profiles";
    private string _reportsUrl = "https://api.amazon.com/profiles";
    private string _dspAdvertiserUrl = "https://api.amazon.com/profiles";


    private readonly AmazonAdsApiOAuth _amazonAdsApiOAuth;

    public AmazonAdsApiServiceTests()
    {
        _httpClientProvider = Substitute.For<IHttpClientProvider>();
        _logMessage = Substitute.For<Action<LogLevel, string>>();
        _logException = Substitute.For<Action<LogLevel, string, Exception>>();

        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        _amazonAdsApiService = new AmazonAdsApiService(serviceArguments);

        _amazonAdsApiOAuth = new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException,
        _httpClientProvider);

        // Set private fields using reflection
        typeof(AmazonAdsApiOAuth).GetField("_accessToken", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, "expiredToken");
        typeof(AmazonAdsApiOAuth).GetField("_accessTokenRetrieved", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, DateTime.Now.AddSeconds(-40));
        typeof(AmazonAdsApiOAuth).GetField("ExpiresIn", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, 100);
    }

    private Credential GetMockCredential()
    {
        dynamic credentialSet = new ExpandoObject();
        credentialSet.ClientId = _clientId;
        credentialSet.ClientSecret = _clientSecret;
        credentialSet.RefreshToken = _refreshToken;
        credentialSet.AuthorizationCode = _authorizationCode;
        credentialSet.RedirectUrl = _redirectUri;
        credentialSet.EndpointUrl = _endpointUrl;
        credentialSet.ProfileUrl = _profileUrl;
        credentialSet.ReportsUrl = _reportsUrl;
        credentialSet.DspAdvertiserUrl = _dspAdvertiserUrl;

        var credential = new Credential
        {
            CredentialSet = credentialSet
        };
        return credential;
    }

    private Credential GetGreenHouseMockCredential()
    {
        dynamic credentialSet = new ExpandoObject();
        credentialSet.Connectionstring = _clientId;
        credentialSet.AccessKey = _clientSecret;
        credentialSet.Region = _refreshToken;
        credentialSet.SecretKey = _authorizationCode;


        var credential = new Credential
        {
            CredentialSet = credentialSet,
            ConnectionString = "testConnectionString"
        };
        return credential;
    }

    private static List<APIReport<ReportSettings>> GetAPIReportSettings()
    {
        var apiReport1 = new APIReport<ReportSettings>
        {
            APIReportName = "Report1",
        };
        var apiReport2 = new APIReport<ReportSettings>
        {
            APIReportName = "Report2",
        };

        List<APIReport<ReportSettings>> listApiReportSettings = new List<APIReport<ReportSettings>>();
        listApiReportSettings.Add(apiReport1);
        listApiReportSettings.Add(apiReport2);

        return listApiReportSettings;
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);
        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullHttpClientProvider_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: null,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.HttpClientProvider");
    }

    [Fact]
    public void Constructor_WithNullCredential_ShouldThrowNullArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: null,
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.Credential");
    }

    [Fact]
    public void Constructor_WithNullGreenhouseS3Credential_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: null,
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.GreenhouseS3Credential");
    }

    [Fact]
    public void Constructor_WithNullIntegration_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: null,
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.Integration");
    }

    [Fact]
    public void Constructor_WithGetS3PathHelper_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: null,
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.GetS3PathHelper");
    }

    [Fact]
    public void Constructor_WithUploadToS3_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: null,
            LogMessage: _logMessage,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.UploadToS3");
    }

    [Fact]
    public void Constructor_WithLogMessage_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: null,
            LogException: _logException
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.LogMessage");
    }

    [Fact]
    public void Constructor_WithLogException_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonAdsApiServiceArguments(
            HttpClientProvider: _httpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            Integration: new Integration { },
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonAdsApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: null
        );

        // Act
        Func<AmazonAdsApiService> act = () => new AmazonAdsApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.LogException");
    }

    [Fact]
    public async Task GetProfilesDataAsync_ReturnsProfileResponses_OnSuccessfulRequest()
    {
        // Arrange
        var mockResponse = new List<ProfileResponse> { new ProfileResponse { ProfileId = "12345" } };
        var mockResponseJson = JsonConvert.SerializeObject(mockResponse);
        string profileUrl = GetMockCredential().CredentialSet.ProfileUrl;

        var requestContent = "key1=value1&key2=value2"; // Adjust this to match your request
        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, _endpointUrl)
        {
            Content = new StringContent(requestContent, Encoding.UTF8, "application/x-www-form-urlencoded")
        };

        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(httpRequestMessage);

        _httpClientProvider.SendRequestAsync(Arg.Any<HttpRequestMessage>())
            .Returns(Task.FromResult(mockResponseJson));


        // Act
        var result = await _amazonAdsApiService.GetProfilesDataAsync(_amazonAdsApiOAuth);

        // Assert
        Assert.NotNull(result);
        Assert.Single(result);
        Assert.Equal("12345", result[0].ProfileId);
    }

    [Fact]
    public async Task GetProfilesDataAsync_ShouldLogError_WhenExceptionIsThrown()
    {
        // Arrange
        var requestContent = "key1=value1&key2=value2"; // Adjust this to match your request
        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, _endpointUrl)
        {
            Content = new StringContent(requestContent, Encoding.UTF8, "application/x-www-form-urlencoded")
        };

        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(httpRequestMessage);

        _httpClientProvider.SendRequestAsync(Arg.Any<HttpRequestMessage>()).Throws(new HttpRequestException("Test Exception"));


        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonAdsApiService.GetProfilesDataAsync(_amazonAdsApiOAuth));
        _logException.Received(1)?.Invoke(LogLevel.Error,
       Arg.Is<string>(msg => msg.Contains("Error while getting the profiles data")),
       Arg.Any<HttpRequestException>());
    }

    [Fact]
    public async Task GetProfilesDataAsync_ShouldReturnEmptyList_WhenResponseIsInvalid()
    {
        // Arrange
        var request = new HttpRequestMessage(HttpMethod.Get, GetMockCredential().CredentialSet.ProfileUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromResult("Invalid Response"));


        // Act
        var result = await _amazonAdsApiService.GetProfilesDataAsync(_amazonAdsApiOAuth);

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result);

        _logMessage.Received(1)?.Invoke(LogLevel.Info, Arg.Is<string>(msg => msg.Contains("Status code showing not successful")));
    }

    [Fact]
    public async Task MakeCreateReportApiCallAsync_ShouldReturnResponseBody_WhenResponseContainsReportId()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReport<ReportSettings> { APIReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        var responseString = "{ \"reportId\": \"12345\" }"; // Valid response with reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.ReportsUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromResult(responseString));

        // Act
        var result = await _amazonAdsApiService.MakeCreateReportApiCallAsync(_amazonAdsApiOAuth, jsonPayload, queueItem, apiReport, cancellableRetry);

        // Assert
        Assert.Equal(responseString, result); // Ensure response body is returned correctly
    }

    [Fact]
    public async Task MakeCreateReportApiCallAsync_ShouldLogError_WhenResponseDoesNotContainReportId()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReport<ReportSettings> { APIReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        var responseString = "{ \"error\": \"something went wrong\" }"; // Invalid response without reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.ReportsUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromResult(responseString));

        // Act
        var result = await _amazonAdsApiService.MakeCreateReportApiCallAsync(_amazonAdsApiOAuth, jsonPayload, queueItem, apiReport, cancellableRetry);

        // Assert
        Assert.Equal(string.Empty, result); // Ensure empty response is returned

        // Verify log message for failed report creation
        _logMessage.Received(1)?.Invoke(LogLevel.Info, Arg.Is<string>(msg => msg.Contains("Create report failed")));
    }

    [Fact]
    public async Task MakeCreateReportApiCallAsync_ShouldLogError_WhenExceptionIsThrown()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReport<ReportSettings> { APIReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request to throw an exception
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.ReportsUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromException<string>(new HttpRequestException("Test exception")));

        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonAdsApiService.MakeCreateReportApiCallAsync(_amazonAdsApiOAuth, jsonPayload, queueItem, apiReport, cancellableRetry));

        // Verify that the exception is logged
        _logException.Received(1)?.Invoke(LogLevel.Error,
            Arg.Is<string>(msg => msg.Contains("Error while creating a report for the profileID")),
            Arg.Any<HttpRequestException>());
    }


    [Fact]
    public async Task GetAdvertiserDataAsync_ShouldReturnResponseBody_WhenResponseContainsReportId()
    {
        // Arrange
        string index = "?startIndex=0&count=100";
        List<Response> responseList = new List<Response>();
        Response response = new Response();
        response.AdvertiserId = "advertiserId";
        response.Name = "name";
        response.Currency = "currency";
        response.Url = "url";
        response.Country = "country";
        response.Timezone = "timezone";
        responseList.Add(response);

        AdvertiserResponse advertiserResponse = new AdvertiserResponse();
        advertiserResponse.Response = responseList;

        var mockResponseJson = JsonConvert.SerializeObject(advertiserResponse);

        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReport<ReportSettings> { APIReportName = "Test Report" };
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Get, GetMockCredential().CredentialSet.DspAdvertiserUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromResult(mockResponseJson));

        // Act
        AdvertiserResponse result = await _amazonAdsApiService.GetAdvertiserDataAsync(_amazonAdsApiOAuth, queueItem
                                                    , cancellableRetry, index);

        // Assert
        Assert.NotNull(result);
        Assert.Single(result.Response);
        Assert.Equal("advertiserId", result.Response.First().AdvertiserId);
    }

    [Fact]
    public async Task GetAdvertiserDataAsync_ShouldLogError_WhenResponseDoesNotContainReportId()
    {
        // Arrange
        string index = "?startIndex=0&count=100";
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };

        var request = new HttpRequestMessage(HttpMethod.Get, GetMockCredential().CredentialSet.DspAdvertiserUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromException<string>(new HttpRequestException("Test Exception")));

        Action<Action> cancellableRetry = action => action();

        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonAdsApiService.GetAdvertiserDataAsync(_amazonAdsApiOAuth, queueItem
                                                , cancellableRetry, index));

        _logException.Received(1)?.Invoke(LogLevel.Error,
       Arg.Is<string>(msg => msg.Contains("Error while getting the advertisers data")),
       Arg.Any<HttpRequestException>());
    }


    [Fact]
    public async Task GetAdvertiserDataAsync_ShouldReturnEmptyList_WhenResponseIsInvalid()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        string index = "?startIndex=0&count=100";

        var request = new HttpRequestMessage(HttpMethod.Get, GetMockCredential().CredentialSet.DspAdvertiserUrl);
        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
                            .Returns(request);
        _httpClientProvider.SendRequestAsync(request).Returns(Task.FromResult("Invalid Response"));

        Action<Action> cancellableRetry = action => action();

        // Act
        AdvertiserResponse result = await _amazonAdsApiService.GetAdvertiserDataAsync(_amazonAdsApiOAuth, queueItem
                                                                , cancellableRetry, index);
        int count = result.TotalResults;
        // Assert
        Assert.NotNull(result);
        Assert.Equal(0, count);

        _logMessage.Received(1)?.Invoke(LogLevel.Info, Arg.Is<string>(msg => msg.Contains("Status code showing not successful")));
    }
}

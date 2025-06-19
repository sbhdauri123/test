using Greenhouse.Auth;
using Greenhouse.DAL.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi;
using Greenhouse.Data.DataSource.AmazonSellingPartnerApi.Responses;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using NLog;
using System.Dynamic;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.Tests.Unit.DataSource.AmazonSellingPartnerApi;

public class AmazonSellingPartnerApiServiceTests
{
    private readonly Action<LogLevel, string>? _logMessage;
    private readonly Action<LogLevel, string, Exception>? _logException;
    private readonly IHttpClientProvider _mockeHttpClientProvider;
    private readonly AmazonSellingPartnerApiService _amazonSellingPartnerApiService;
    private AmazonSellingPartnerApiServiceArguments serviceArguments;

    private string _clientId = "testClientId";
    private string _clientSecret = "testClientSecret";
    private string _refreshToken = "testRefreshToken";
    private string _endpointUrl = "https://api.amazon.com/auth/o2/token";
    private string _createReportsUrl = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports";
    private string _downloadReportUrl = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents";
    private readonly ITokenApiClient _tokenApiClient;

    public AmazonSellingPartnerApiServiceTests()
    {
        _mockeHttpClientProvider = Substitute.For<IHttpClientProvider>();
        _logMessage = Substitute.For<Action<LogLevel, string>>();
        _logException = Substitute.For<Action<LogLevel, string, Exception>>();
        _tokenApiClient = Substitute.For<ITokenApiClient>();

        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { },
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        _amazonSellingPartnerApiService = new AmazonSellingPartnerApiService(serviceArguments);
    }

    private Credential GetMockCredential()
    {
        dynamic credentialSet = new ExpandoObject();
        credentialSet.ClientId = _clientId;
        credentialSet.ClientSecret = _clientSecret;
        credentialSet.RefreshToken = _refreshToken;
        credentialSet.EndpointUrl = _endpointUrl;
        credentialSet.CreateReportsUrl = _createReportsUrl;
        credentialSet.DownloadReportUrl = _downloadReportUrl;

        var credential = new Credential
        {
            CredentialSet = credentialSet
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
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);
        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullHttpClientProvider_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: null,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.HttpClientProvider");
    }

    [Fact]
    public void Constructor_WithNullCredential_ShouldThrowNullArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: null,
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.Credential");
    }

    [Fact]
    public void Constructor_WithNullGreenhouseS3Credential_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: null,
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.GreenhouseS3Credential");
    }

    [Fact]
    public void Constructor_WithGetS3PathHelper_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: null,
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.GetS3PathHelper");
    }

    [Fact]
    public void Constructor_WithUploadToS3_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: null,
            LogMessage: _logMessage,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.UploadToS3");
    }

    [Fact]
    public void Constructor_WithLogMessage_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: null,
            LogException: _logException,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.LogMessage");
    }

    [Fact]
    public void Constructor_WithLogException_ShouldThrowArgumentNullException()
    {
        serviceArguments = new AmazonSellingPartnerApiServiceArguments(
            HttpClientProvider: _mockeHttpClientProvider,
            Credential: GetMockCredential(),
            GreenhouseS3Credential: GetMockCredential(),
            GetS3PathHelper: (id, date, fileName) => $"s3://bucket/path/{fileName}",
            ApiReports: (IEnumerable<APIReport<Data.DataSource.AmazonSellingPartnerApi.ReportSettings>>)GetAPIReportSettings(),
            UploadToS3: (file, s3File, path, size, flag) => { /* Mock implementation */ },
            LogMessage: _logMessage,
            LogException: null,
            TokenApiClient: _tokenApiClient
        );

        // Act
        Func<AmazonSellingPartnerApiService> act = () => new AmazonSellingPartnerApiService(serviceArguments);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceArguments.LogException");
    }

    [Fact]
    public async Task MakeCreateReportApiCallAsync_ShouldReturnResponseBody_WhenResponseContainsReportId()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReport<ReportSettings> { APIReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        CreateReportResponse responseString = new CreateReportResponse { ReportId = "12345" }; // Valid response with reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<CreateReportResponse>(request).Returns(Task.FromResult(responseString));

        // Act
        var result = await _amazonSellingPartnerApiService.RequestReportAsync(jsonPayload, queueItem, apiReport, cancellableRetry);

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
        CreateReportResponse responseString = new CreateReportResponse(); // Invalid response without reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<CreateReportResponse>(request).Returns(Task.FromResult(responseString));

        // Act
        var result = await _amazonSellingPartnerApiService.RequestReportAsync(jsonPayload, queueItem, apiReport, cancellableRetry);

        // Assert
        Assert.Null(result); // Ensure empty response is returned

        // Verify log message for failed report creation
        _logMessage.Received(1)?.Invoke(LogLevel.Info, Arg.Is<string>(msg => msg.Contains("Create report failed for the marketPlaceId")));
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
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<CreateReportResponse>(request).Returns(Task.FromException<CreateReportResponse>(new HttpRequestException("Test exception")));

        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonSellingPartnerApiService.RequestReportAsync(jsonPayload, queueItem, apiReport, cancellableRetry));

        // Verify that the exception is logged
        _logException.Received(1)?.Invoke(LogLevel.Error,
            Arg.Is<string>(msg => msg.Contains("Error while creating a report for the marketPlaceId")),
            Arg.Any<HttpRequestException>());
    }

    [Fact]
    public async Task MakeReportStatusApiCallAsync_ShouldReturnResponseBody_WhenResponseContainsReportId()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReportItem { ReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        ReportProcessingStatus responseString = new ReportProcessingStatus { ReportDocumentId = "12345" };  // Valid response with reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<ReportProcessingStatus>(request).Returns(responseString);

        // Act
        var result = await _amazonSellingPartnerApiService.GetReportStatusAndDocumentIdAsync(jsonPayload, queueItem, apiReport, cancellableRetry);

        // Assert
        Assert.Equal(responseString.ReportDocumentId, result.ReportDocumentId); // Ensure response body is returned correctly
    }

    [Fact]
    public async Task MakeReportStatusApiCallAsync_ShouldLogError_WhenResponseDoesNotContainReportId()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReportItem { ReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        ReportProcessingStatus responseString = new ReportProcessingStatus(); // Invalid response without reportId
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request and response
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<ReportProcessingStatus>(request).Returns(Task.FromResult(responseString));

        // Act
        var result = await _amazonSellingPartnerApiService.GetReportStatusAndDocumentIdAsync(jsonPayload, queueItem, apiReport, cancellableRetry);

        // Assert
        Assert.Null(result.ReportDocumentId); // Ensure empty response is returned

        // Verify log message for failed report creation
        _logMessage.Received(1)?.Invoke(LogLevel.Info, Arg.Is<string>(msg => msg.Contains("Checking report status failed for the marketPlaceId")));
    }

    [Fact]
    public async Task MakeReportStatusApiCallAsync_ShouldLogError_WhenExceptionIsThrown()
    {
        // Arrange
        var queueItem = new OrderedQueue { EntityID = "testEntityId" };
        var apiReport = new APIReportItem { ReportName = "Test Report" };
        var jsonPayload = "{ \"key\": \"value\" }";
        var cancellableRetry = new Action<Action>(action => action()); // Mock retry action

        // Mock the request to throw an exception
        var request = new HttpRequestMessage(HttpMethod.Post, GetMockCredential().CredentialSet.CreateReportsUrl);
        _mockeHttpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(request);
        _mockeHttpClientProvider.SendRequestAndDeserializeAsync<ReportProcessingStatus>(request).Returns(Task.FromException<ReportProcessingStatus>(new HttpRequestException("Test exception")));

        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonSellingPartnerApiService.GetReportStatusAndDocumentIdAsync(jsonPayload, queueItem, apiReport, cancellableRetry));

        // Verify that the exception is logged
        _logException.Received(1)?.Invoke(LogLevel.Error,
            Arg.Is<string>(msg => msg.Contains("Error while checking report status for the marketPlaceId")),
            Arg.Any<HttpRequestException>());
    }
}

using Greenhouse.Auth;
using Greenhouse.DAL.DataSource.LinkedIn;
using Greenhouse.Utilities;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.Tests.Unit.DataSource.LinkedIn;

public class ApiClientTests
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly ITokenApiClient _tokenApiClient;
    private readonly ApiClientOptions _options;
    private readonly ApiClient _apiClient;

    public ApiClientTests()
    {
        _httpClientProvider = Substitute.For<IHttpClientProvider>();
        _tokenApiClient = Substitute.For<ITokenApiClient>();
        _options = new ApiClientOptions { EndpointUri = "https://api.linkedin.com", PageSize = 100 };
        _apiClient = new ApiClient(_options, _httpClientProvider, _tokenApiClient);
        _tokenApiClient.GetAccessTokenAsync(Arg.Any<TokenDataSource>()).Returns("test_access_token");
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        Func<ApiClient> act = () => new ApiClient(_options, _httpClientProvider, _tokenApiClient);

        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullHttpClientProvider_ShouldThrowArgumentNullException()
    {
        Func<ApiClient> act = () => new ApiClient(_options, null, _tokenApiClient);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("httpClientProvider");
    }

    [Fact]
    public void Constructor_WithNullTokenApiClient_ShouldThrowArgumentNullException()
    {
        Func<ApiClient> act = () => new ApiClient(_options, _httpClientProvider, null);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("tokenApiClient");
    }

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<ApiClient> act = () => new ApiClient(null, _httpClientProvider, _tokenApiClient);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Constructor_WithNullOrWhiteSpaceEndpointUri_ShouldThrowArgumentException(string? endpointUri)
    {
        // Arrange
        ApiClientOptions invalidOptions = new() { EndpointUri = endpointUri };

        // Act
        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _tokenApiClient);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ApiClientOptions.EndpointUri));
    }

    [Fact]
    public void Constructor_WithInvalidEndpointUri_ShouldThrowUriFormatException()
    {
        ApiClientOptions invalidOptions = new() { EndpointUri = "not a valid uri", PageSize = 100 };

        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _tokenApiClient);

        act.Should().Throw<UriFormatException>();
    }

    [Fact]
    public void Constructor_WithNegativePageSize_ShouldThrowArgumentOutOfRangeException()
    {
        ApiClientOptions invalidOptions = new() { EndpointUri = "https://api.linkedin.com", PageSize = -1 };

        Func<ApiClient> act = () => new ApiClient(invalidOptions, _httpClientProvider, _tokenApiClient);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(ApiClientOptions.PageSize));
    }

    #endregion

    #region Fact Report Tests

    [Fact]
    public async Task DownloadFactReportStreamAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<Stream>> act = async () => await _apiClient.DownloadFactReportStreamAsync(null);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task DownloadFactReportStreamAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
        string? accountId)
    {
        // Arrange
        FactReportDownloadOptions options = new()
        {
            AccountId = accountId,
            FileDate = new DateTime(2024, 7, 31),
            DeliveryPath = "adAnalytics",
            ReportFieldNames = ["clicks", "impressions"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadFactReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(FactReportDownloadOptions.AccountId));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task DownloadFactReportStreamAsync_WithNullOrWhiteSpaceDeliveryPath_ShouldThrowArgumentException(
        string? deliveryPath)
    {
        // Arrange
        FactReportDownloadOptions options = new()
        {
            AccountId = "123456",
            FileDate = new DateTime(2024, 7, 31),
            DeliveryPath = deliveryPath,
            ReportFieldNames = ["clicks", "impressions"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadFactReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(FactReportDownloadOptions.DeliveryPath));
    }

    [Fact]
    public async Task DownloadFactReportStreamAsync_WithNullReportFieldNames_ShouldThrowArgumentNullException()
    {
        // Arrange
        FactReportDownloadOptions options = new()
        {
            AccountId = "123456",
            FileDate = new DateTime(2024, 7, 31),
            DeliveryPath = "adAnalytics",
            ReportFieldNames = null
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadFactReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentNullException>()
            .WithParameterName(nameof(FactReportDownloadOptions.ReportFieldNames));
    }

    [Fact]
    public async Task DownloadFactReportStreamAsync_SendsCorrectRequest()
    {
        // Arrange
        FactReportDownloadOptions options = new()
        {
            AccountId = "123456",
            FileDate = new DateTime(2024, 7, 31),
            DeliveryPath = "adAnalytics",
            ReportFieldNames = ["clicks", "impressions"]
        };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadFactReportStreamAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.AuthScheme.Should().Be("Bearer");
        capturedRequest?.AuthToken.Should().Be("test_access_token");
        capturedRequest?.Headers.Should().Contain("Linkedin-Version", "202404");
        capturedRequest?.Headers.Should().Contain("X-HTTP-Method-Override", "GET");
        capturedRequest?.Headers.Should().Contain("User-Agent", "Publicis");
        capturedRequest?.Headers.Should()
            .Contain(new KeyValuePair<string, string>("X-Restli-Protocol-Version", "2.0.0"));
        capturedRequest?.Uri.Should().Be("https://api.linkedin.com/adAnalytics");

        if (capturedRequest?.Content is not null)
        {
            string content = await capturedRequest.Content.ReadAsStringAsync();
            content.Should().Be("accounts=List(urn%3Ali%3AsponsoredAccount%3A123456)"
                                + "&dateRange=(start:(day:31,month:7,year:2024),end:(day:31,month:7,year:2024))"
                                + "&pivot=CREATIVE&q=analytics&timeGranularity=DAILY&fields=clicks,impressions");
        }
    }

    #endregion

    #region AdAccounts Report Tests

    [Fact]
    public async Task DownloadAdAccountsReportStreamAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdAccountsReportStreamAsync(null);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task DownloadAdAccountsReportStreamAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
        string? accountId)
    {
        // Arrange
        AdAccountsReportDownloadOptions options = new() { AccountId = accountId };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdAccountsReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(AdAccountsReportDownloadOptions.AccountId));
    }

    [Fact]
    public async Task DownloadAdAccountsReportStreamAsync_SendsCorrectRequest()
    {
        // Arrange
        AdAccountsReportDownloadOptions options = new() { AccountId = "789012" };

        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadAdAccountsReportStreamAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.AuthScheme.Should().Be("Bearer");
        capturedRequest?.AuthToken.Should().Be("test_access_token");
        capturedRequest?.Headers.Should().Contain("Linkedin-Version", "202404");
        capturedRequest?.Headers.Should().Contain("X-HTTP-Method-Override", "GET");
        capturedRequest?.Headers.Should().Contain("User-Agent", "Publicis");
        capturedRequest?.Headers.Should()
            .Contain(new KeyValuePair<string, string>("X-Restli-Protocol-Version", "2.0.0"));
        capturedRequest?.Uri.Should().Be("https://api.linkedin.com/adAccounts/789012");

        if (capturedRequest?.Content is not null)
        {
            string content = await capturedRequest.Content.ReadAsStringAsync();
            content.Should().BeEmpty();
        }
    }

    #endregion

    #region AdCampaignGroups Report Tests

    [Fact]
    public async Task DownloadAdCampaignGroupsReportStreamAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignGroupsReportStreamAsync(null);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadAdCampaignGroupsReportStreamAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
            string? accountId)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = accountId,
            DeliveryPath = "adCampaignGroups",
            SearchIds = ["group1", "group2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignGroupsReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.AccountId));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadAdCampaignGroupsReportStreamAsync_WithNullOrWhiteSpaceDeliveryPath_ShouldThrowArgumentException(
            string? deliveryPath)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = "345678",
            DeliveryPath = deliveryPath,
            SearchIds = ["group1", "group2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignGroupsReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.DeliveryPath));
    }

    [Theory]
    [ClassData(typeof(DownloadAdCampaignGroupsReportTestData))]
    public async Task DownloadAdCampaignGroupsReportStreamAsync_SendsCorrectRequest(
        DimensionReportDownloadOptions options, string expectedBody)
    {
        // Arrange
        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadAdCampaignGroupsReportStreamAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.AuthScheme.Should().Be("Bearer");
        capturedRequest?.AuthToken.Should().Be("test_access_token");
        capturedRequest?.Headers.Should().Contain("Linkedin-Version", "202404");
        capturedRequest?.Headers.Should().Contain("X-HTTP-Method-Override", "GET");
        capturedRequest?.Headers.Should().Contain("User-Agent", "Publicis");
        capturedRequest?.Headers.Should()
            .Contain(new KeyValuePair<string, string>("X-Restli-Protocol-Version", "2.0.0"));
        capturedRequest?.Uri.Should()
            .Be("https://api.linkedin.com/adAccounts/345678/adCampaignGroups");

        if (capturedRequest?.Content is not null)
        {
            string content = await capturedRequest.Content.ReadAsStringAsync();
            content.Should().Be(expectedBody);
        }
    }

    #endregion

    #region AdCampaign Report Tests

    [Fact]
    public async Task DownloadAdCampaignsReportStreamAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignsReportStreamAsync(null);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadAdCampaignsReportStreamAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
            string? accountId)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = accountId,
            DeliveryPath = "adCampaigns",
            SearchIds = ["campaign1", "campaign2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignsReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.AccountId));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadAdCampaignsReportStreamAsync_WithNullOrWhiteSpaceDeliveryPath_ShouldThrowArgumentException(
            string? deliveryPath)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = "345678",
            DeliveryPath = deliveryPath,
            SearchIds = ["campaign1", "campaign2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadAdCampaignsReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.DeliveryPath));
    }

    [Theory]
    [ClassData(typeof(DownloadAdCampaignsReportTestData))]
    public async Task DownloadAdCampaignsReportStreamAsync_SendsCorrectRequest(DimensionReportDownloadOptions options,
        string expectedBody)
    {
        // Arrange
        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadAdCampaignsReportStreamAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.AuthScheme.Should().Be("Bearer");
        capturedRequest?.AuthToken.Should().Be("test_access_token");
        capturedRequest?.Headers.Should().Contain("Linkedin-Version", "202404");
        capturedRequest?.Headers.Should().Contain("X-HTTP-Method-Override", "GET");
        capturedRequest?.Headers.Should().Contain("User-Agent", "Publicis");
        capturedRequest?.Headers.Should()
            .Contain(new KeyValuePair<string, string>("X-Restli-Protocol-Version", "2.0.0"));
        capturedRequest?.Uri.Should().Be("https://api.linkedin.com/adAccounts/901234/adCampaigns");

        if (capturedRequest?.Content is not null)
        {
            string content = await capturedRequest.Content.ReadAsStringAsync();
            content.Should().Be(expectedBody);
        }
    }

    #endregion

    #region Creatives Report Tests

    [Fact]
    public async Task DownloadCreativesReportStreamAsync_WithNullOptions_ShouldThrowArgumentNullException()
    {
        Func<Task<Stream>> act = async () => await _apiClient.DownloadCreativesReportStreamAsync(null);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("options");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadCreativesReportStreamAsync_WithNullOrWhiteSpaceAccountId_ShouldThrowArgumentException(
            string? accountId)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = accountId,
            DeliveryPath = "creatives",
            SearchIds = ["creative1", "creative2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadCreativesReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.AccountId));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task
        DownloadCreativesReportStreamAsync_WithNullOrWhiteSpaceDeliveryPath_ShouldThrowArgumentException(
            string? deliveryPath)
    {
        // Arrange
        DimensionReportDownloadOptions options = new()
        {
            AccountId = "345678",
            DeliveryPath = deliveryPath,
            SearchIds = ["creative1", "creative2"]
        };

        // Act
        Func<Task<Stream>> act = async () => await _apiClient.DownloadCreativesReportStreamAsync(options);

        // Assert
        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName(nameof(DimensionReportDownloadOptions.DeliveryPath));
    }

    [Theory]
    [ClassData(typeof(DownloadCreativesReportTestData))]
    public async Task DownloadCreativesReportStreamAsync_SendsCorrectRequest(DimensionReportDownloadOptions options,
        string expectedBody)
    {
        // Arrange
        HttpRequestOptions? capturedRequest = null;
        _httpClientProvider.DownloadFileStreamAsync(Arg.Do<HttpRequestOptions>(r => capturedRequest = r))
            .Returns(new MemoryStream());

        // Act
        await _apiClient.DownloadCreativesReportStreamAsync(options);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest?.Content.Should().NotBeNull();
        capturedRequest?.Method.Should().Be(HttpMethod.Post);
        capturedRequest?.ContentType.Should().Be("application/x-www-form-urlencoded");
        capturedRequest?.AuthScheme.Should().Be("Bearer");
        capturedRequest?.AuthToken.Should().Be("test_access_token");
        capturedRequest?.Headers.Should().Contain("Linkedin-Version", "202404");
        capturedRequest?.Headers.Should().Contain("X-HTTP-Method-Override", "GET");
        capturedRequest?.Headers.Should().Contain("User-Agent", "Publicis");
        capturedRequest?.Headers.Should()
            .Contain(new KeyValuePair<string, string>("X-Restli-Protocol-Version", "2.0.0"));
        capturedRequest?.Uri.Should().Be("https://api.linkedin.com/adAccounts/567890/creatives");

        if (capturedRequest?.Content is not null)
        {
            string content = await capturedRequest.Content.ReadAsStringAsync();
            content.Should().Be(expectedBody);
        }
    }

    #endregion
}

#region Theory Test Data

public sealed class
    DownloadAdCampaignGroupsReportTestData : TheoryData<DimensionReportDownloadOptions, string>
{
    public DownloadAdCampaignGroupsReportTestData()
    {
        Add(
            new DimensionReportDownloadOptions
            {
                AccountId = "345678",
                DeliveryPath = "adCampaignGroups",
                SearchIds = ["group1", "group2"],
            },
            "q=search&search=(test:false)&pageSize=100");

        Add(
            new DimensionReportDownloadOptions
            {
                AccountId = "345678",
                DeliveryPath = "adCampaignGroups",
                SearchIds = ["group1", "group2"],
                NextPageToken = "next_page_token_123"
            },
            "q=search&search=(test:false)&pageSize=100&pageToken=next_page_token_123");
    }
}

public sealed class DownloadAdCampaignsReportTestData : TheoryData<DimensionReportDownloadOptions, string>
{
    public DownloadAdCampaignsReportTestData()
    {
        Add(
            new DimensionReportDownloadOptions()
            {
                AccountId = "901234",
                DeliveryPath = "adCampaigns",
                SearchIds = ["campaign1", "campaign2"]
            },
            "q=search&search=(test:false,campaignGroup:(values:List(urn%3Ali%3AsponsoredCampaignGroup%3Acampaign1"
            + ",urn%3Ali%3AsponsoredCampaignGroup%3Acampaign2)))"
            + "&pageSize=100");

        Add(
            new DimensionReportDownloadOptions()
            {
                AccountId = "901234",
                DeliveryPath = "adCampaigns",
                SearchIds = ["campaign1", "campaign2"],
                NextPageToken = "next_page_token_123"
            },
            "q=search&search=(test:false,campaignGroup:(values:List(urn%3Ali%3AsponsoredCampaignGroup%3Acampaign1"
            + ",urn%3Ali%3AsponsoredCampaignGroup%3Acampaign2)))"
            + "&pageSize=100&pageToken=next_page_token_123");
    }
}

public sealed class DownloadCreativesReportTestData : TheoryData<DimensionReportDownloadOptions, string>
{
    public DownloadCreativesReportTestData()
    {
        Add(
            new DimensionReportDownloadOptions
            {
                AccountId = "567890",
                DeliveryPath = "creatives",
                SearchIds = ["creative1", "creative2"]
            },
            "q=criteria&isTestAccount=false&campaigns=List(urn%3Ali%3AsponsoredCampaign%3Acreative1,urn%3Ali%3AsponsoredCampaign%3Acreative2)&pageSize=100");

        Add(
            new DimensionReportDownloadOptions
            {
                AccountId = "567890",
                DeliveryPath = "creatives",
                SearchIds = ["creative1", "creative2"],
                NextPageToken = "next_page_token_123"
            },
            "q=criteria&isTestAccount=false&campaigns=List(urn%3Ali%3AsponsoredCampaign%3Acreative1,urn%3Ali%3AsponsoredCampaign%3Acreative2)&pageSize=100&pageToken=next_page_token_123");
    }
}

#endregion
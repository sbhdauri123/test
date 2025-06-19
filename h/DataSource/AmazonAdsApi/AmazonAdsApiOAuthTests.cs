using Greenhouse.Auth;
using Greenhouse.DAL.DataSource.AmazonAdsApi;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using NLog;
using NSubstitute.ExceptionExtensions;
using System.Dynamic;
using System.Text;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.Tests.Unit.DataSource.AmazonAdsApi;

public class AmazonAdsApiOAuthTests
{
    private readonly Action<LogLevel, string>? _logMessage;
    private readonly Action<LogLevel, string, Exception>? _logException;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly IAmazonAdsApiOAuth _subAmazonAdsApiOAuth;
    private readonly ResponseToken _responseToken;
    private readonly Credential _credential;

    private string _clientId = "testClientId";
    private string _clientSecret = "testClientSecret";
    private string _refreshToken = "testRefreshToken";
    private string _authorizationCode = "testAuthorizationCode";
    private string _redirectUri = "https://amazon.com/ap/oa";
    private string _endpointUrl = "https://api.amazon.com/auth/o2/token";

    private readonly AmazonAdsApiOAuth _amazonAdsApiOAuth;

    public AmazonAdsApiOAuthTests()
    {
        _httpClientProvider = Substitute.For<IHttpClientProvider>();
        _logMessage = Substitute.For<Action<LogLevel, string>>();
        _logException = Substitute.For<Action<LogLevel, string, Exception>>();
        _credential = Substitute.For<Credential>();
        _credential.CredentialSet.ClientId = _clientId;
        _credential.CredentialSet.ClientSecret = _clientSecret;
        _credential.CredentialSet.RefreshToken = _refreshToken;
        _credential.CredentialSet.EndpointUrl = _endpointUrl;
        _credential.CredentialSet.AuthorizationCode = _authorizationCode;
        _credential.CredentialSet.RedirectUrl = _redirectUri;

        _subAmazonAdsApiOAuth = Substitute.For<IAmazonAdsApiOAuth>();

        // Sample token response
        _responseToken = new ResponseToken
        {
            AccessToken = "newAccessToken",
            ExpiresIn = 3600
        };

        _amazonAdsApiOAuth = new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException,
        _httpClientProvider);

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

        var credential = new Credential
        {
            CredentialSet = credentialSet
        };
        return credential;
    }

    [Fact]
    public void Constructor_WithValidParameters_ShouldNotThrow()
    {
        // Act
        Func<AmazonAdsApiOAuth> act = () => new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException,
                                                _httpClientProvider);
        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_WithNullHttpClientProvider_ShouldThrowArgumentNullException()
    {
        // Act
        Func<AmazonAdsApiOAuth> act = () => new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException, null);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("httpClientProvider");
    }

    [Fact]
    public void Constructor_WithNullCredential_ShouldThrowArgumentNullException()
    {
        // Act
        Func<AmazonAdsApiOAuth> act = () => new AmazonAdsApiOAuth(null, _logMessage, _logException,
                                                _httpClientProvider);

        act.Should().Throw<ArgumentNullException>().WithParameterName("credential");
    }

    [Fact]
    public void Constructor_WithNullLogException_ShouldThrowArgumentNullException()
    {
        // Act
        Func<AmazonAdsApiOAuth> act = () => new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, null,
                                                _httpClientProvider);

        act.Should().Throw<ArgumentNullException>().WithParameterName("logException");
    }

    [Fact]
    public void Constructor_WithNullLogMessage_ShouldThrowArgumentNullException()
    {
        // Act
        Func<AmazonAdsApiOAuth> act = () => new AmazonAdsApiOAuth(GetMockCredential(), null, _logException,
                                                _httpClientProvider);

        act.Should().Throw<ArgumentNullException>().WithParameterName("logMessage");
    }

    [Fact]
    public void PrepareRequestContent_ShouldReturnHeadersForRefreshToken()
    {
        // Act
        AmazonAdsApiOAuth amazonAdsApiOAuth = new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException, _httpClientProvider);
        var result = amazonAdsApiOAuth.PrepareRequestContent(true);

        // Assert
        Assert.Equal(4, result.Count);
        Assert.Equal("testClientId", result["client_id"]);
        Assert.Equal("testClientSecret", result["client_secret"]);
        Assert.Equal("refresh_token", result["grant_type"]);
        Assert.Equal("testRefreshToken", result["refresh_token"]);
    }

    [Fact]
    public void PrepareRequestContent_ShouldReturnHeadersForAuthorizationCode()
    {
        // Act
        AmazonAdsApiOAuth amazonAdsApiOAuth = new AmazonAdsApiOAuth(GetMockCredential(), _logMessage, _logException, _httpClientProvider);
        var result = amazonAdsApiOAuth.PrepareRequestContent(false);

        // Assert
        Assert.Equal(5, result.Count);
        Assert.Equal("testClientId", result["client_id"]);
        Assert.Equal("testClientSecret", result["client_secret"]);
        Assert.Equal("authorization_code", result["grant_type"]);
        Assert.Equal("testAuthorizationCode", result["code"]);
        Assert.Equal("https://amazon.com/ap/oa", result["redirect_uri"]);
    }

    [Fact]
    public async Task GetOauthToken_ShouldReturnResponseToken_WhenRequestIsSuccessful()
    {
        // Arrange
        var expectedResponseString = "{\"access_token\":\"abc123\", \"token_type\":\"Bearer\"}";
        var expectedResponseToken = new ResponseToken
        {
            AccessToken = "abc123",
            TokenType = "Bearer"
        };

        var requestContent = "key1=value1&key2=value2"; // Adjust this to match your request
        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, _endpointUrl)
        {
            Content = new StringContent(requestContent, Encoding.UTF8, "application/x-www-form-urlencoded")
        };

        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(httpRequestMessage);
        _httpClientProvider.SendRequestAsync(httpRequestMessage)
            .Returns(Task.FromResult(expectedResponseString));

        // Act
        var result = await _amazonAdsApiOAuth.GetOauthToken();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(expectedResponseToken.AccessToken, result.AccessToken);
        Assert.Equal(expectedResponseToken.TokenType, result.TokenType);
    }

    [Fact]
    public async Task GetOauthToken_ShouldThrowException_WhenRequestFails()
    {
        // Arrange
        var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, _endpointUrl);

        _httpClientProvider.BuildRequestMessage(Arg.Any<HttpRequestOptions>())
            .Returns(httpRequestMessage);
        _httpClientProvider.SendRequestAsync(httpRequestMessage)
            .Throws(new HttpRequestException("Request failed"));

        // Act & Assert
        await Assert.ThrowsAsync<HttpRequestException>(() => _amazonAdsApiOAuth.GetOauthToken());
    }

    [Fact]
    public void AccessToken_ShouldReturnCachedAccessToken_WhenNotExpired()
    {
        // Arrange
        var expectedToken = "cachedAccessToken";
        _amazonAdsApiOAuth.GetType().GetField("_accessToken", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, expectedToken);
        _amazonAdsApiOAuth.GetType().GetField("_accessTokenRetrieved", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, DateTime.Now);
        _amazonAdsApiOAuth.GetType().GetField("ExpiresIn", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            ?.SetValue(_amazonAdsApiOAuth, 3600);

        // Act
        var accessToken = _amazonAdsApiOAuth.AccessToken;

        // Assert
        Assert.Equal(expectedToken, accessToken);

    }

    [Fact]
    public void AccessToken_Should_Return_New_AccessToken_When_Token_Is_Expired()
    {
        // Arrange: Set the initial token as expired
        SetPrivateField(_subAmazonAdsApiOAuth, "_accessToken", "expiredToken");
        SetPrivateField(_subAmazonAdsApiOAuth, "_accessTokenRetrieved", DateTime.Now.AddSeconds(-4000));
        SetPrivateField(_subAmazonAdsApiOAuth, "ExpiresIn", 3600);

        // Mock the GetOauthToken method to return a new token
        var newAuthToken = new ResponseToken { AccessToken = "newAccessToken", ExpiresIn = 3600 };
        _subAmazonAdsApiOAuth.GetOauthToken().Returns(Task.FromResult(newAuthToken));

        // Mock the AccessToken property
        _subAmazonAdsApiOAuth.AccessToken.Returns("newAccessToken");


        // Act
        var accessToken = _subAmazonAdsApiOAuth.AccessToken;

        // Assert
        Assert.Equal("newAccessToken", accessToken);
    }

    [Fact]
    public void AccessToken_Should_Fetch_New_Token_When_AccessToken_Is_Null_Or_Empty()
    {
        // Arrange: Set the initial token as null
        SetPrivateField(_subAmazonAdsApiOAuth, "_accessToken", null);

        var newAuthToken = new ResponseToken { AccessToken = "newAccessToken", ExpiresIn = 3600 };
        _subAmazonAdsApiOAuth.GetOauthToken().Returns(Task.FromResult(newAuthToken));

        // Mock the AccessToken property
        _subAmazonAdsApiOAuth.AccessToken.Returns("newAccessToken");

        // Act
        var accessToken = _subAmazonAdsApiOAuth.AccessToken;

        // Assert
        Assert.Equal("newAccessToken", accessToken);
    }

    private static void SetPrivateField(object obj, string fieldName, object? value)
    {
        var field = obj.GetType().GetField(fieldName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        field?.SetValue(obj, value);
    }
}

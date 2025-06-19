using Greenhouse.Auth;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.AmazonAdsApi;

public class AmazonAdsApiOAuth : IAmazonAdsApiOAuth
{
    internal int ExpiresIn = 2588400;    //29 days, 23 hours
    private readonly string _clientId;
    private readonly string _clientSecret;
    private readonly string _refreshToken;
    private readonly string _endpointTokenURI;
    private readonly string _redirect_uri;
    internal string _accessToken;
    private readonly string _authorizationCode;
    internal DateTime _accessTokenRetrieved;
    private readonly Action<LogLevel, string> _logMessage;
    private readonly Action<LogLevel, string, Exception> _logException;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly Credential _credential;

    public string AccessToken
    {
        get
        {
            if (string.IsNullOrEmpty(_accessToken) || DateTime.Now > _accessTokenRetrieved.AddSeconds(ExpiresIn))
            {
                _accessTokenRetrieved = DateTime.Now;
                var auth = GetOauthToken();
                _accessToken = auth?.GetAwaiter().GetResult().AccessToken;
                ExpiresIn = auth?.GetAwaiter().GetResult().ExpiresIn ?? ExpiresIn;
            }

            return _accessToken;
        }
    }

    public AmazonAdsApiOAuth(Credential credential, Action<LogLevel, string> logMessage
                            , Action<LogLevel, string, Exception> logException
                            , IHttpClientProvider httpClientProvider)
    {
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentNullException.ThrowIfNull(logMessage);
        ArgumentNullException.ThrowIfNull(logException);
        ArgumentNullException.ThrowIfNull(httpClientProvider);

        _clientId = credential.CredentialSet.ClientId;
        _clientSecret = credential.CredentialSet.ClientSecret;
        _refreshToken = credential.CredentialSet.RefreshToken;
        _endpointTokenURI = credential.CredentialSet.EndpointUrl;
        _authorizationCode = credential.CredentialSet.AuthorizationCode;
        _redirect_uri = credential.CredentialSet.RedirectUrl;
        _logMessage = logMessage;
        _logException = logException;
        _httpClientProvider = httpClientProvider;
        _credential = credential;
    }

    public async Task<ResponseToken> GetOauthToken()
    {
        string postData = string.Join("&", PrepareRequestContent(true).Select(kvp => $"{kvp.Key}={kvp.Value}"));
        var httpRequestMessageSettings = new HttpRequestOptions()
        {
            Uri = _endpointTokenURI.ToString(),
            ContentType = "application/x-www-form-urlencoded",
            Method = HttpMethod.Post,
            Content = new StringContent(postData, Encoding.UTF8, "application/x-www-form-urlencoded")
        };
        var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);
        var responseString = await _httpClientProvider.SendRequestAsync(request);
        return JsonConvert.DeserializeObject<ResponseToken>(responseString);
    }

    public Dictionary<string, string> PrepareRequestContent(bool isRefreshToken)
    {
        Dictionary<string, string> headers = new Dictionary<string, string>();

        headers.Add("client_id", _clientId);
        headers.Add("client_secret", _clientSecret);

        if (isRefreshToken)
        {
            headers.Add("grant_type", "refresh_token");
            headers.Add("refresh_token", _refreshToken);
        }
        else
        {
            headers.Add("grant_type", "authorization_code");
            headers.Add("code", _authorizationCode);
            headers.Add("redirect_uri", _redirect_uri);
        }
        return headers;
    }
}

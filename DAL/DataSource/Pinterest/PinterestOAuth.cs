using Greenhouse.Auth;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Pinterest
{
    public class PinterestOAuth
    {
        private int ExpiresIn = 2588400; //29 days, 23 hours
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly string _refreshToken;
        private readonly Uri _endpointURI;
        private string _accessToken;
        private DateTime _accessTokenRetrieved;

        public string AccessToken
        {
            get
            {
                if (!string.IsNullOrEmpty(_accessToken) && DateTime.Now <= _accessTokenRetrieved.AddSeconds(ExpiresIn))
                {
                    return _accessToken;
                }

                _accessTokenRetrieved = DateTime.Now;
                ResponseToken auth = GetOauthTokenAsync().GetAwaiter().GetResult();
                _accessToken = auth?.AccessToken;
                ExpiresIn = auth?.ExpiresIn ?? ExpiresIn;

                return _accessToken;
            }
        }

        public PinterestOAuth(IHttpClientProvider httpClientProvider, Data.Model.Setup.Credential credential)
        {
            _httpClientProvider = httpClientProvider;
            _clientId = credential.CredentialSet.ClientId;
            _clientSecret = credential.CredentialSet.ClientSecret;
            _refreshToken = HttpUtility.UrlDecode(credential.CredentialSet.RefreshToken);
            _endpointURI = new Uri(credential.CredentialSet.EndpointURI);
        }

        private async Task<ResponseToken> GetOauthTokenAsync()
        {
            string clientIdAndClientSecretBase64 =
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_clientId}:{_clientSecret}"));

            return await _httpClientProvider.SendRequestAndDeserializeAsync<ResponseToken>(new HttpRequestOptions
            {
                Uri = _endpointURI.ToString(),
                Method = HttpMethod.Post,
                AuthToken = null,
                AuthScheme = null,
                ContentType = null,
                Content =
                    new StringContent($"grant_type=refresh_token&refresh_token={_refreshToken}", Encoding.UTF8,
                        "application/x-www-form-urlencoded"),
                Headers = new Dictionary<string, string>
                {
                    { "Authorization", $"Basic {clientIdAndClientSecretBase64}" }, { "User-Agent", "Publicis" }
                }
            });
        }
    }
}
using Greenhouse.Auth;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Retargetly
{
    public class RetargetlyOAuth
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly Credential _credential;
        private readonly Integration _integration;
        private DateTime _accessTokenExpiration;
        private ResponseToken _responseToken;

        public RetargetlyOAuth(IHttpClientProvider httpClientProvider, Credential credential, Integration integration)
        {
            _httpClientProvider = httpClientProvider;
            _credential = credential;
            _integration = integration;
        }

        public string AccessToken
        {
            get
            {
                if (_responseToken != null && DateTime.Now <= _accessTokenExpiration)
                {
                    return _responseToken.AccessToken;
                }

                _responseToken = GetOAuthTokenAsync().GetAwaiter().GetResult();
                _accessTokenExpiration = DateTime.Now.AddSeconds(_responseToken.ExpiresIn);

                return _responseToken.AccessToken;
            }
        }

        private async Task<ResponseToken> GetOAuthTokenAsync()
        {
            return await _httpClientProvider.SendRequestAndDeserializeAsync<ResponseToken>(new HttpRequestOptions
            {
                Uri = string.Concat(_integration.EndpointURI, "/login"),
                Method = HttpMethod.Post,
                AuthToken = null,
                AuthScheme = null,
                ContentType = null,
                Content = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>
                {
                    new("username", _credential.CredentialSet.Username),
                    new("password", _credential.CredentialSet.Password)
                }),
                Headers = new Dictionary<string, string> { { "x-api-key", _credential.CredentialSet.XApiKey } }
            });
        }
    }
}
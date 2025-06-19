using Greenhouse.Auth;
using Greenhouse.Utilities;
using System;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Skai
{
    public class SkaiOAuth
    {
        private string _accessToken;
        private DateTime _accessTokenRetrieved;
        private readonly object _authLock = new();
        private readonly string _clientId;
        private readonly string _refreshToken;
        private readonly string _endpointUri;
        private readonly IHttpClientProvider _httpClientProvider;
        public int ExpiresIn { get; set; } = 1800;

        public SkaiOAuth(Greenhouse.Data.Model.Setup.Credential credentials, string endpointUri,
            IHttpClientProvider httpClientProvider)
        {
            _clientId = credentials.CredentialSet.id;
            _refreshToken = credentials.CredentialSet.token;
            _httpClientProvider = httpClientProvider;
            _endpointUri = endpointUri;
        }

        public string SkaiAccessToken
        {
            get
            {
                if (!string.IsNullOrEmpty(_accessToken) && DateTime.Now <= _accessTokenRetrieved.AddSeconds(ExpiresIn))
                {
                    return _accessToken;
                }

                lock (_authLock)
                {
                    if (!string.IsNullOrEmpty(_accessToken) &&
                        DateTime.Now <= _accessTokenRetrieved.AddSeconds(ExpiresIn))
                    {
                        return _accessToken;
                    }

                    _accessTokenRetrieved = DateTime.Now;
                    ResponseToken auth = GetSkaiAuthTokenAsync().GetAwaiter().GetResult();
                    _accessToken = auth?.AccessToken;
                    ExpiresIn = auth?.ExpiresIn ?? 1800;
                }

                return _accessToken;
            }
        }

        private async Task<ResponseToken> GetSkaiAuthTokenAsync()
        {
            return await _httpClientProvider.SendRequestAndDeserializeAsync<ResponseToken>(new HttpRequestOptions
            {
                Uri = $"{_endpointUri}/token",
                Method = HttpMethod.Post,
                ContentType = MediaTypeNames.Application.FormUrlEncoded,
                Content = new StringContent($"client_id={_clientId}&refresh_token={_refreshToken}", Encoding.UTF8,
                    MediaTypeNames.Application.FormUrlEncoded),
            });
        }
    }
}
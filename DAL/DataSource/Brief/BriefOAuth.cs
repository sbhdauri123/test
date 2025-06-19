using Greenhouse.Auth;
using Greenhouse.Common.Exceptions;
using Greenhouse.Utilities;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Brief
{
    public class BriefOAuth
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        private string AccessToken;
        public int ExpiresIn { get; set; } = 1800;
        public Uri EndpointURI { get; set; }
        public string Host { get; set; }
        private DateTime AccessTokenRetrieved { get; set; }
        private readonly IHttpClientProvider _httpClientProvider;

        public BriefOAuth(IHttpClientProvider httpClientProvider, Greenhouse.Data.Model.Setup.Credential credentials)
        {
            _httpClientProvider = httpClientProvider;
            this.ClientId = credentials.CredentialSet.ClientId;
            this.ClientSecret = credentials.CredentialSet.ClientSecret;
            this.Host = credentials.CredentialSet.Host;
            this.EndpointURI = new Uri($"{credentials.CredentialSet.Host.TrimEnd('/')}/{credentials.CredentialSet.Endpoint.TrimEnd('/')}");
        }

        public string BriefAccessToken
        {
            get
            {
                if (!string.IsNullOrEmpty(AccessToken) && DateTime.Now <= AccessTokenRetrieved.AddSeconds(ExpiresIn))
                {
                    return AccessToken;
                }

                AccessTokenRetrieved = DateTime.Now;
                ResponseToken auth = GetBriefAuthTokenAsync().GetAwaiter().GetResult();
                AccessToken = auth?.AccessToken;
                ExpiresIn = auth?.ExpiresIn ?? 3600;
                return AccessToken;
            }
        }

        private async Task<ResponseToken> GetBriefAuthTokenAsync()
        {
            if (EndpointURI == null)
                throw new EndpointNotSetException("Endpoint not set.");

            return await _httpClientProvider.SendRequestAndDeserializeAsync<ResponseToken>(new HttpRequestOptions
            {
                Method = HttpMethod.Post,
                Uri = EndpointURI.ToString(),
                Content = new StringContent($"clientId={ClientId}&clientSecret={ClientSecret}", Encoding.UTF8,
                    "application/x-www-form-urlencoded"),
                ContentType = "application/x-www-form-urlencoded",
            });
        }
    }
}

using Greenhouse.Auth;
using Greenhouse.Common.Exceptions;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Brief
{
    public class DataManagerOAuth
    {
        private readonly IHttpClientProvider _httpClientProvider;
        public string Username { get; set; }
        public string Password { get; set; }
        public string GrantType { get; set; }
        private string AccessToken;
        public int ExpiresIn { get; set; } = 1800;
        public Uri EndpointURI { get; set; }
        private DateTime AccessTokenRetrieved { get; set; }

        public DataManagerOAuth(IHttpClientProvider httpClientProvider, Greenhouse.Data.Model.Setup.Credential credentials, string endpoint)
        {
            _httpClientProvider = httpClientProvider;

            if (((IDictionary<String, object>)credentials.CredentialSet).ContainsKey("Username"))
            {
                this.Username = credentials.CredentialSet.Username;
            }

            if (((IDictionary<String, object>)credentials.CredentialSet).ContainsKey("Password"))
            {
                this.Password = credentials.CredentialSet.Password;
            }

            if (((IDictionary<String, object>)credentials.CredentialSet).ContainsKey("GrantType"))
            {
                this.GrantType = credentials.CredentialSet.GrantType;
            }

            if (((IDictionary<String, object>)credentials.CredentialSet).ContainsKey("Path"))
            {
                this.EndpointURI = new Uri($"{endpoint.TrimEnd('/')}/{credentials.CredentialSet.Path.TrimEnd('/')}");
            }
        }

        public string DataManagerAccessToken
        {
            get
            {
                if (!string.IsNullOrEmpty(AccessToken) && DateTime.Now <= AccessTokenRetrieved.AddSeconds(ExpiresIn))
                {
                    return AccessToken;
                }

                AccessTokenRetrieved = DateTime.Now;
                var auth = this.GetDataManagerAuthTokenAsync().GetAwaiter().GetResult();
                AccessToken = auth?.AccessToken;
                ExpiresIn = auth?.ExpiresIn ?? 3600;
                return AccessToken;
            }
        }

        private async Task<ResponseToken> GetDataManagerAuthTokenAsync()
        {
            if (EndpointURI == null)
                throw new EndpointNotSetException("Endpoint not set.");

            return await _httpClientProvider.SendRequestAndDeserializeAsync<ResponseToken>(new HttpRequestOptions
            {
                Method = HttpMethod.Post,
                Uri = EndpointURI.ToString(),
                Content = new StringContent($"username={Username}&password={Password}&grant_type={GrantType}",
                    Encoding.UTF8, "application/x-www-form-urlencoded"),
                ContentType = "application/x-www-form-urlencoded",
            });
        }
    }
}

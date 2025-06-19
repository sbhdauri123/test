using Greenhouse.Auth;
using Greenhouse.Common.Exceptions;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Snapchat;

public class SnapchatOAuth
{
    public string ClientId { get; set; }
    public string ClientSecret { get; set; }
    public string RefreshToken { get; set; }
    private string AccessToken;
    public int ExpiresIn { get; set; } = 1800;
    public Uri EndpointURI { get; set; }

    private IHttpClientProvider _httpClientProvider;

    private DateTime AccessTokenRetrieved { get; set; }


    public SnapchatOAuth(Greenhouse.Data.Model.Setup.Credential credentials, IHttpClientProvider httpClientProvider)
    {
        this.ClientId = credentials.CredentialSet.ClientId;
        this.ClientSecret = credentials.CredentialSet.ClientSecret;
        this.RefreshToken = credentials.CredentialSet.RefreshToken;
        //Strangely Snapchat Auth is a POST command, but requires the parameters to be submitted as QS parameters.
        this.EndpointURI = new Uri($"{credentials.CredentialSet.Endpoint}?{this.GetOAuthPostBody()}");
        _httpClientProvider = httpClientProvider;
    }

    public async Task<string> GetSnapChatAccessTokenAsync()
    {
        if (string.IsNullOrEmpty(AccessToken) || DateTime.Now > AccessTokenRetrieved.AddSeconds(ExpiresIn))
        {
            AccessTokenRetrieved = DateTime.Now;
            var auth = await GetSnapchatAuthTokenAsync();
            AccessToken = auth?.AccessToken;
            ExpiresIn = auth?.ExpiresIn ?? 1800;
        }
        return AccessToken;
    }


    private async Task<ResponseToken> GetSnapchatAuthTokenAsync()
    {
        if (EndpointURI == null)
            throw new EndpointNotSetException("Endpoint not set.");

        try
        {
            var postData = GetOAuthPostBody();
            var httpRequestMessageSettings = new HttpRequestOptions()
            {
                Uri = EndpointURI.ToString(),
                ContentType = "application/x-www-form-urlencoded",
                Method = HttpMethod.Post,
                Content = new StringContent(postData, Encoding.UTF8, "application/x-www-form-urlencoded")
            };
            var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);
            var responseString = await _httpClientProvider.SendRequestAsync(request);
            return JsonConvert.DeserializeObject<ResponseToken>(responseString);
        }
        catch (HttpClientProviderRequestException ex)
        {
            throw new APIResponseException($"Failed to retrieve Snapchat Auth token. Exception details : {ex}", ex);
        }
    }

    public string GetOAuthPostBody()
    {
        var postData = String.Format("client_id={0}&client_secret={1}&refresh_token={2}&grant_type=refresh_token", ClientId, ClientSecret, RefreshToken);
        return postData;
    }
}

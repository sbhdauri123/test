using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Pinterest
{
    public class ApiClient : IApiClient
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly PinterestOAuth _oAuth;
        private readonly Integration _currentIntegration;

        private string ContentType { get; set; } = "application/json";

        private string UriPath
        {
            get
            {
                return new Uri($"{_currentIntegration.EndpointURI.TrimEnd('/')}").ToString();
            }
        }

        public ApiClient(IHttpClientProvider httpClientProvider, PinterestOAuth oAuth, Integration integration)
        {
            _httpClientProvider = httpClientProvider;
            _oAuth = oAuth;
            _currentIntegration = integration;
        }

        public async Task<T> GetReportDownloadUrl<T>(DownloadReportOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            options.Validate();

            var request = BuildHttpRequestMessage(HttpMethod.Get, options.UriPath, _oAuth.AccessToken);
            var response = await _httpClientProvider.GetResponseAsync(request);

            string stringResponse = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<T>(stringResponse);
        }

        public async Task<Stream> DownloadReportAsync(DownloadReportOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            options.Validate();

            var request = BuildHttpRequestMessage(HttpMethod.Get, options.UriPath, null);
            var response = await _httpClientProvider.DownloadFileStreamAsync(request);

            return response;
        }

        public async Task<T> RequestApiReportAsync<T>(RequestApiReportOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            options.Validate();

            var request = BuildHttpRequestMessage(options.MethodType, BuildUri(options.UrlExtension),
                _oAuth.AccessToken, options.Content);
            var response = await _httpClientProvider.GetResponseAsync(request);

            string stringResponse = await response.Content.ReadAsStringAsync();
            T deserializedResponse = JsonConvert.DeserializeObject<T>(stringResponse);

            deserializedResponse.GetType().GetProperty("RawJson")?.SetValue(deserializedResponse, stringResponse, null);

            var headersDictionary = new Dictionary<string, string>();
            foreach (var header in response.Headers)
            {
                headersDictionary[header.Key] = string.Join(", ", header.Value);
            }

            foreach (var header in response.Content.Headers)
            {
                headersDictionary[header.Key] = string.Join(", ", header.Value);
            }

            deserializedResponse.GetType().GetProperty("Header")
                ?.SetValue(deserializedResponse, headersDictionary, null); //Asking for dictionary header 
            deserializedResponse.GetType().GetProperty("ResponseCode")
                ?.SetValue(deserializedResponse, response.StatusCode, null);
            return deserializedResponse;
        }


        private HttpRequestOptions BuildHttpRequestMessage(HttpMethod httpMethod, string uri, string auth,
            string body = null)
        {
            HttpContent content = null;

            if (httpMethod == HttpMethod.Post && !string.IsNullOrEmpty(body))
            {
                content = new StringContent(body, Encoding.UTF8, ContentType);
            }

            return new HttpRequestOptions
            {
                Uri = uri,
                Method = httpMethod,
                AuthToken = auth,
                ContentType = ContentType,
                Content = content,
            };
        }

        private string BuildUri(string path) => $"{UriPath}/{path}".TrimEnd('/');
    }
}
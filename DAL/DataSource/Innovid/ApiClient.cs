using Greenhouse.Data.DataSource.Innovid;
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

namespace Greenhouse.DAL.DataSource.Innovid
{
    public class ApiClient : IApiClient
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly Credential _credential;
        private string AuthToken
        {
            get
            {
                return Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes($"{_credential.CredentialSet.UserName}:{_credential.CredentialSet.Password}"));
            }
        }

        private string UriPath
        {
            get
            {
                return new Uri($"{_credential.CredentialSet.Endpoint.TrimEnd('/')}").ToString();
            }
        }


        public ApiClient(IHttpClientProvider httpClientProvider, Credential credential)
        {
            _httpClientProvider = httpClientProvider;
            _credential = credential;
        }

        public async Task<ReportStatusData> GetReportStatusAsync(GetReportStatusOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            options.Validate();

            var request = BuildHttpRequestOptions(HttpMethod.Get, BuildUri(options.UrlExtension), "application/json", string.Empty);
            var response = await _httpClientProvider.SendRequestAsync(request);

            if (response.Contains(options.PropertyName))
                return JsonConvert.DeserializeObject<ReportStatusData>(response);
            return null;
        }

        public async Task<ReportRequest> RequestReportAsync(RequestReportOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();

            string jsonBody = JsonConvert.SerializeObject(options.Content, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            var request = BuildHttpRequestOptions(HttpMethod.Post, BuildUri(options.UrlExtension), "application/json", jsonBody);
            var response = await _httpClientProvider.SendRequestAsync(request);

            return JsonConvert.DeserializeObject<ReportRequest>(response);
        }

        public async Task<Stream> DownloadReportAsync(DownloadReportOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            options.Validate();

            var request = BuildHttpRequestOptions(HttpMethod.Get, options.UriPath, "application/json");
            var response = await _httpClientProvider.DownloadFileStreamAsync(request);

            return response;
        }

        public async Task<ClientData> GetAdvertisersAsync(GetAdvertisersOptions options)
        {
            var request = BuildHttpRequestOptions(HttpMethod.Get, BuildUri(options.UrlExtension), "application/json", string.Empty);
            var response = await _httpClientProvider.SendRequestAsync(request);

            return JsonConvert.DeserializeObject<ClientData>(response);
        }

        private HttpRequestOptions BuildHttpRequestOptions(HttpMethod httpMethod, string uri, string contentType, string content)
        {
            return new HttpRequestOptions
            {
                Uri = uri,
                ContentType = contentType,
                Content = new StringContent(content, Encoding.UTF8, contentType),
                Method = httpMethod,
                Headers = new Dictionary<string, string>
                {
                    { "Accept", "*/*" },
                    {"Authorization", $"Basic {AuthToken}" }
                }
            };
        }

        private static HttpRequestOptions BuildHttpRequestOptions(HttpMethod httpMethod, string uri, string contentType)
        {
            return new HttpRequestOptions { Uri = uri, ContentType = contentType, Method = httpMethod };
        }

        private string BuildUri(string path) => $"{UriPath}/{path}".TrimEnd('/');
    }
}

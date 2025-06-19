using Greenhouse.Utilities;
using System.Net.Http;
using System.Text;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.FRED
{
    public abstract class BaseRequest
    {
        private readonly IHttpClientProvider _httpClientProvider;
        public string EndpointUri { get; set; }
        public string URLExtension { get; set; }
        public string ApiKey { get; set; }
        public string JobLogGuid { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Get;
        public abstract string UriPath { get; }

        protected BaseRequest(IHttpClientProvider httpClientProvider)
        {
            _httpClientProvider = httpClientProvider;
        }

        public virtual async System.Threading.Tasks.Task<T> FetchDataAsync<T>(string bodyRequest = null)
        {
            string endpoint = $"{EndpointUri}/{UriPath}";
            HttpContent content = null;

            if (MethodType == HttpMethod.Post && !string.IsNullOrEmpty(bodyRequest))
            {
                content = new StringContent(bodyRequest, Encoding.UTF8, "application/json");
            }

            return await _httpClientProvider.SendRequestAndDeserializeAsync<T>(new HttpRequestOptions
            {
                Uri = endpoint,
                Method = MethodType,
                AuthToken = ApiKey,
                ContentType = "application/json",
                Content = content,
            });
        }
    }
}

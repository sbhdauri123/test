using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Core
{
    public abstract class BaseApiRequest : IApiRequest
    {
        private readonly IHttpClientProvider _httpClientProvider;
        public virtual string ContentType { get; set; } = "application/json";
        public string NextPageUrl { get; set; }
        public abstract string AccessToken { get; }
        public HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public abstract string UriPath { get; }
        public abstract string HttpBody { get; }
        public virtual Dictionary<string, string> Headers { get; set; } = new();

        protected BaseApiRequest(IHttpClientProvider httpClientProvider)
        {
            _httpClientProvider = httpClientProvider;
        }

        /// <summary>
        /// Set "readHeaders" parameter to TRUE and deserialize JSON into object and assigns following object properties: "RawJson", "Header" and "ResponseCode"
        /// </summary>
        public virtual async Task<TResponse> FetchDataAsync<TResponse>(bool readHeaders = false)
        {
            HttpContent content = null;

            if (MethodType == HttpMethod.Post && !string.IsNullOrEmpty(HttpBody))
            {
                content = new StringContent(this.HttpBody, Encoding.UTF8, ContentType);
            }

            HttpRequestOptions requestOptions = new()
            {
                Uri = UriPath,
                ContentType = ContentType,
                AuthToken = AccessToken,
                Content = content,
                Method = MethodType
            };

            if (readHeaders)
            {
                return await _httpClientProvider.SendRequestAndDeserializeAsync<TResponse>(requestOptions);
            }

            string response = await _httpClientProvider.SendRequestAsync(requestOptions);

            return JsonConvert.DeserializeObject<TResponse>(response);
        }

        public virtual async Task<TResponse> FetchDataAsync<TResponse>(Action<Stream> saveFileAction)
        {
            HttpContent content = null;

            if (MethodType == HttpMethod.Post && !string.IsNullOrEmpty(HttpBody))
            {
                content = new StringContent(this.HttpBody, Encoding.UTF8, ContentType);
            }

            await using Stream responseStream = await _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions()
            {
                Uri = UriPath,
                AuthToken = AccessToken,
                ContentType = ContentType,
                Method = MethodType,
                Content = content,
            });

            using StreamReader reader = new(responseStream);
            string result = await reader.ReadToEndAsync();

            responseStream.Seek(0, SeekOrigin.Begin);
            saveFileAction(responseStream);

            if (typeof(TResponse) == typeof(string))
            {
                return (TResponse)(object)result;
            }

            return JsonConvert.DeserializeObject<TResponse>(result);
        }
    }
}

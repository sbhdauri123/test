using Greenhouse.Utilities;
using Newtonsoft.Json;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Skai
{
    public abstract class BaseRequest
    {
        public string EndpointUri { get; set; }
        public string URLExtension { get; set; }
        public string ContentType { get; set; } = "application/json";
        public SkaiOAuth Credential { get; set; }
        public HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public abstract string UriPath { get; }
        public abstract string HttpBody { get; }
        public IHttpClientProvider HttpClientProvider { get; set; }

        public virtual async Task<TResponse> FetchDataAsync<TResponse>()
        {
            HttpContent content = null;

            if (MethodType == HttpMethod.Post && !string.IsNullOrEmpty(HttpBody))
            {
                content = new StringContent(this.HttpBody, Encoding.UTF8, ContentType);
            }

            string response = await HttpClientProvider.SendRequestAsync(new HttpRequestOptions
            {
                Uri = UriPath,
                Method = MethodType,
                AuthToken = Credential.SkaiAccessToken,
                ContentType = ContentType,
                Content = content
            });

            return JsonConvert.DeserializeObject<TResponse>(response);
        }

        public virtual async Task<Stream> FetchDataAsync(CancellationToken cancellationToken = default)
        {
            HttpContent content = null;

            if (MethodType == HttpMethod.Post && !string.IsNullOrEmpty(HttpBody))
            {
                content = new StringContent(HttpBody, Encoding.UTF8, ContentType);
            }

            return await HttpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
            {
                Uri = UriPath,
                Method = MethodType,
                AuthToken = Credential.SkaiAccessToken,
                ContentType = ContentType,
                Content = content
            }, cancellationToken);
        }
    }
}
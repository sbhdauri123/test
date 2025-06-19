using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.DAL.Databricks
{
    using Greenhouse.Utilities;
    using System.IO;

    public class DataBricksRequest
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly Data.Model.Setup.Credential _credential;
        private readonly HttpMethod _method;

        public DataBricksRequest(IHttpClientProvider httpClientProvider, Data.Model.Setup.Credential credential, HttpMethod method)
        {
            _httpClientProvider = httpClientProvider;
            _credential = credential;
            _method = method;
        }

        /// <summary>
        /// POST request. No exception handling done. It's up to the consumer to handle any exception, and to dispose of the Response object
        /// </summary>
        /// <param name="path"></param>
        /// <param name="body"></param>
        public HttpWebResponse PostRequest(string path, string body)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_credential.CredentialSet.Endpoint + path);

            request.Headers.Add("Authorization", $"Bearer {_credential.CredentialSet.AuthToken}");
            request.ContentType = "application/json";
            request.Accept = "application/json";
            request.Headers.Set(HttpRequestHeader.CacheControl, "no-cache");
            request.Headers.Set(HttpRequestHeader.AcceptEncoding, "gzip, deflate");
            request.KeepAlive = false;
            request.Method = System.Net.Http.HttpMethod.Post.Method;
            request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

            byte[] postBytes = System.Text.Encoding.UTF8.GetBytes(body);
            request.ContentLength = postBytes.Length;
            Stream stream = request.GetRequestStream();
            stream.Write(postBytes, 0, postBytes.Length);
            stream.Close();

            return (HttpWebResponse)request.GetResponse();
        }

        /// <summary>
        /// GET request object. No exception handling done. It's up to the consumer to handle any exception
        /// </summary>
        /// <param name="path"></param>
        public HttpWebRequest GetRequest(string path)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_credential.CredentialSet.Endpoint + path);
            request.Headers.Add("Authorization", $"Bearer {_credential.CredentialSet.AuthToken}");
            request.ContentType = "application/json";
            request.Accept = "application/json";
            request.Headers.Set(HttpRequestHeader.CacheControl, "no-cache");
            request.Headers.Set(HttpRequestHeader.AcceptEncoding, "gzip, deflate");
            request.KeepAlive = false;
            request.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

            request.Method = _method.Method;
            request.ServicePoint.Expect100Continue = false;
            return request;
        }

        /// <summary>
        /// Send Databricks API Requests using HttpClient
        /// </summary>
        public async Task<string> SendRequestAsync(string path, string bodyRequest)
        {
            HttpContent content = null;

            if (_method == HttpMethod.Post && !string.IsNullOrEmpty(bodyRequest))
            {
                content = new StringContent(bodyRequest, Encoding.UTF8, "application/json");
            }

            return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
            {
                Uri = $"{_credential.CredentialSet.Endpoint}/{path}",
                ContentType = "application/json",
                AuthToken = _credential.CredentialSet.AuthToken,
                Content = content,
                Method = _method
            });
        }
    }
}

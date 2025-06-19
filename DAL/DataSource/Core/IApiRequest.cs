using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Core
{
    public interface IApiRequest
    {
        public string ContentType { get; set; }
        public string NextPageUrl { get; set; }
        public abstract string AccessToken { get; }
        public HttpMethod MethodType { get; set; }
        public abstract string UriPath { get; }
        public abstract string HttpBody { get; }
        Task<TResponse> FetchDataAsync<TResponse>(bool readHeaders = false);
        Task<TResponse> FetchDataAsync<TResponse>(Action<Stream> saveFileAction);
    }
}

using Greenhouse.Data.DataSource.Reddit;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;

namespace Greenhouse.DAL.DataSource.Reddit
{
    [Serializable]
    public class ApiRequest : Core.BaseApiRequest
    {
        private readonly RedditOAuth _credential;
        private readonly string _endpointUri;
        private readonly string _urlExtension;

        public string AccountID { get; set; }
        public string Parameters { get; private set; }
        public MetricsRequestBody BodyRequest { get; set; }

        public ApiRequest(IHttpClientProvider httpClientProvider, string endpointUri, dynamic ApiKey, string urlExtension) : base(httpClientProvider)
        {
            _endpointUri = endpointUri;
            _credential = ApiKey;
            _urlExtension = urlExtension;
        }

        public override string UriPath
        {
            get
            {
                if (!string.IsNullOrEmpty(NextPageUrl))
                    return NextPageUrl;

                string endpoint = string.IsNullOrEmpty(_urlExtension) ?
                    $"{_endpointUri.TrimEnd('/')}/{AccountID}" :
                    $"{_endpointUri.TrimEnd('/')}/{AccountID}/{_urlExtension}";

                var uriBuilder = new UriBuilder(endpoint)
                {
                    Query = Parameters
                };

                return uriBuilder.ToString();
            }
        }

        public void SetParameters(int pageSize)
        {
            Parameters = $"page.size={pageSize}";
        }

        public override string HttpBody
        {
            get
            {
                if (BodyRequest != null)
                    return JsonConvert.SerializeObject(BodyRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                return JsonConvert.SerializeObject(BodyRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            }
        }

        public override string AccessToken => _credential.AccessToken;
        public ApiReportItem ApiReportItem { get; set; }
    }
}

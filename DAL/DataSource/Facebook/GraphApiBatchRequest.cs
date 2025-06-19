using Greenhouse.Data.DataSource.Facebook;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.Facebook
{
    public class GraphApiBatchRequest : GraphApiBaseRequest
    {
        public override string UriPath
        {
            get
            {
                return $"{URLExtension}";
            }
        }
        public List<BatchOperation> BatchOperations { get; set; }

        public override string HttpBody
        {
            get
            {
                var batchRequest = new Batch { batch = BatchOperations };

                return Newtonsoft.Json.JsonConvert.SerializeObject(batchRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            }
        }

        public GraphApiBatchRequest(IHttpClientProvider httpClientProvider, string endpointUri, string jobLogGuid, string apiKey, string urlExtension) : base(httpClientProvider)
        {
            this.EndpointUri = endpointUri;
            this.JobLogGuid = jobLogGuid;
            this.ApiKey = apiKey;
            this.URLExtension = urlExtension;
            this.BatchOperations = new List<BatchOperation>();
            this.MethodType = System.Net.Http.HttpMethod.Post;
        }
    }
}

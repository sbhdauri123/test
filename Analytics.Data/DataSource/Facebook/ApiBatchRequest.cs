using Newtonsoft.Json;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Facebook
{
    public class BatchOperation
    {
        public string method { get; set; }
        public string relative_url { get; set; }
    }
    public class Batch
    {
        public List<BatchOperation> batch { get; set; }
    }
    public class ApiBatchRequest
    {
        public string ApiVersion { get; set; }
        public List<BatchOperation> BatchOperations { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public string GetBatchRequestBody()
        {
            var batchRequest = new Batch { batch = BatchOperations };

            return Newtonsoft.Json.JsonConvert.SerializeObject(batchRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        public string UriPath { get; }
        public int Index { get; set; }
    }
}

using Greenhouse.Common;
using Greenhouse.Data.DataSource.Skai.AsyncReport;
using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Greenhouse.DAL.DataSource.Skai
{
    [Serializable]
    public class ApiRequest : BaseRequest
    {
        public string ServerID { get; set; }
        public string ReportID { get; set; }
        public bool IsStatusCheck { get; set; }
        public string Parameters { get; private set; }
        public string ProfileID { get; set; }
        public AsyncRequest BodyRequestAsync { get; set; }
        public SyncReportRequest BodyRequest { get; set; }

        public ApiRequest(string endpointUri, dynamic ApiKey, string urlExtension, IHttpClientProvider httpClientProvider)
        {
            this.EndpointUri = endpointUri;
            this.Credential = ApiKey;
            this.URLExtension = urlExtension;
            this.HttpClientProvider = httpClientProvider;
        }

        public override string UriPath
        {
            get
            {
                if (IsStatusCheck)
                {
                    base.MethodType = System.Net.Http.HttpMethod.Get;
                    return $"{this.EndpointUri}/{URLExtension}/{ReportID}/status?ks={ServerID}";
                }
                else
                {
                    var path = string.IsNullOrEmpty(ReportID) ? $"{URLExtension}" : $"{URLExtension}/{ReportID}";
                    return string.IsNullOrEmpty(Parameters) ? $"{this.EndpointUri}/{path}" : $"{this.EndpointUri}/{path}?{Parameters.TrimStart(Constants.AMPERSAND_ARRAY)}";
                }
            }
        }

        public void SetParameters()
        {
            var parameters = new List<string>();

            if (ServerID != null)
            {
                parameters.Add($"ks={ServerID}");
            }

            if (ProfileID != null)
            {
                parameters.Add($"profile_id={ProfileID}");
            }

            Parameters = string.Join("&", parameters);
        }

        public override string HttpBody
        {
            get
            {
                if (BodyRequest != null)
                    return JsonConvert.SerializeObject(BodyRequest, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                return JsonConvert.SerializeObject(BodyRequestAsync, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            }
        }
    }
}
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System.Linq;
using System.Reflection;

namespace Greenhouse.DAL.DataSource.FRED
{
    public class ApiRequest : BaseRequest
    {
        public ApiRequest(string endpointUri, string jobLogGuid, dynamic ApiKey, string urlExtension, IHttpClientProvider httpClientProvider) : base(httpClientProvider)
        {
            this.EndpointUri = endpointUri;
            this.JobLogGuid = jobLogGuid;
            this.ApiKey = ApiKey;
            this.URLExtension = urlExtension;
        }

        public override string UriPath
        {
            get
            {
                return string.IsNullOrEmpty(_parameters) ? $"{URLExtension}?api_key={ApiKey}" : $"{URLExtension}?api_key={ApiKey}&{_parameters}";
            }
        }
        private string _parameters { get; set; }

        public void SetParameters<T>(T reportParameters)
        {
            _parameters = string.Join("&",
                reportParameters.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(r => r.GetValue(reportParameters) != null)
                .Select(x => new
                {
                    parameter = x.GetCustomAttribute<JsonPropertyAttribute>().PropertyName,
                    parameterValue = x.GetValue(reportParameters)
                })
                .Select(y => $"{y.parameter}={y.parameterValue}"));
        }
    }
}

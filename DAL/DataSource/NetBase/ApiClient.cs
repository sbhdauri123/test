using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using static Greenhouse.Data.DataSource.NetBase.Core.NetBaseEnums;
using static Greenhouse.Data.DataSource.NetBase.Data.MetricValues.MetricValuesParameters;
using static Greenhouse.Data.DataSource.NetBase.Data.Topics.TopicsParameters;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.NetBase
{
    public class ApiClient : IApiClient
    {
        private readonly IHttpClientProvider _httpClientProvider;
        private Integration _integration;
        private readonly NetBaseOAuth _oAuth;
        private readonly int _urlMaxLength;

        private string Parameters { get; set; }
        private string UriPath => _integration.EndpointURI;
        private string AuthToken => _oAuth.AccessToken;

        public ApiClient(IHttpClientProvider httpClientProvider, Integration integration, NetBaseOAuth oAuth, int urlMaxLength)
        {
            _httpClientProvider = httpClientProvider;
            _oAuth = oAuth;
            _integration = integration;
            _urlMaxLength = urlMaxLength;
        }

        public async Task<T> FetchDataAsync<T>(FetchDataOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();

            SetParameters(options.ReportParameters, options.StartDate, options.EndDate);

            var request = BuildHttpRequestMessage(HttpMethod.Get, BuildUri(BuiltUrlExtesion(options.UrlExtension)), "application/json");
            var response = await _httpClientProvider.GetResponseAsync(request);

            string stringResponse = await response.Content.ReadAsStringAsync();
            T deserializedResponse = JsonConvert.DeserializeObject<T>(stringResponse);

            deserializedResponse.GetType().GetProperty("RawJson")?.SetValue(deserializedResponse, stringResponse, null);
            deserializedResponse.GetType().GetProperty("ResponseCode")?.SetValue(deserializedResponse, response.StatusCode, null);
            try
            {
                deserializedResponse.GetType().GetProperty("Header")?.SetValue(deserializedResponse, response.Headers, null);
            }
            catch
            {
                var headersDictionary = new Dictionary<string, string>();
                foreach (var header in response.Headers)
                {
                    headersDictionary[header.Key] = string.Join(", ", header.Value);
                }
                foreach (var header in response.Content.Headers)
                {
                    headersDictionary[header.Key] = string.Join(", ", header.Value);
                }

                deserializedResponse.GetType().GetProperty("Header")?.SetValue(deserializedResponse, headersDictionary, null);//Asking for dictionary header 
            }

            return deserializedResponse;
        }

        public async Task<string> FetchRawDataAsync(FetchDataOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            options.Validate();
            SetParameters(options.ReportParameters, options.StartDate, options.EndDate);

            var request = BuildHttpRequestMessage(HttpMethod.Get, BuildUri(BuiltUrlExtesion(options.UrlExtension)), "application/json");
            return await _httpClientProvider.SendRequestAsync(request);
        }

        private HttpRequestOptions BuildHttpRequestMessage(HttpMethod httpMethod, string uri, string contentType)
        {
            //The Insight API allows a maximum of 8,192 characters in a call.
            if (uri.Length > _urlMaxLength)
            {
                throw new NotSupportedException($"URL is over the character limit of {_urlMaxLength} - URL: {uri}");
            };

            return new HttpRequestOptions
            {
                Uri = uri,
                ContentType = contentType,
                Method = httpMethod,
                AuthToken = AuthToken
            };
        }

        private void SetParameters<T>(T reportParameters, string startDate = null, string endDate = null)
        {
            var parameters = new List<string>();

            var type = reportParameters.GetType();
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            foreach (PropertyInfo property in properties)
            {
                var propValue = property.GetValue(reportParameters);
                if (propValue == null) continue;
                var propName = property.GetCustomAttribute<JsonPropertyAttribute>().PropertyName;
                var propType = property.PropertyType;
                if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    var values = (List<string>)property.GetValue(reportParameters, null);

                    //need to de-duplicate list from Api Report Settings as the api charges one call per metric series:
                    var distinctValues = values.Select(x => x.ToLower()).Distinct();
                    foreach (var value in distinctValues)
                    {
                        switch (propName)
                        {
                            case "metricSeries":
                                parameters.Add($"{propName}={UtilsText.ConvertToEnum<MetricSeriesEnum>(value.ToString())}");
                                break;
                            case "genders":
                                parameters.Add($"{propName}={UtilsText.ConvertToEnum<GendersEnum>(value.ToString())}");
                                break;
                            case "sentiments":
                                parameters.Add($"{propName}={UtilsText.ConvertToEnum<SentimentsEnum>(value.ToString())}");
                                break;
                            case "sources":
                                parameters.Add($"{propName}={UtilsText.ConvertToEnum<SourcesEnum>(value.ToString())}");
                                break;
                            default:
                                parameters.Add($"{propName}={value}");
                                break;
                        }
                    }
                }
                else
                {
                    switch (propName)
                    {
                        case "timeUnits":
                            parameters.Add($"{propName}={UtilsText.ConvertToEnum<TimeUnitsEnum>(propValue.ToString())}");
                            break;
                        case "dateRange":
                            parameters.Add($"{propName}={UtilsText.ConvertToEnum<DateRangeEnum>(propValue.ToString())}");
                            break;
                        case "contentType":
                            parameters.Add($"{propName}={UtilsText.ConvertToEnum<ContentTypeEnum>(propValue.ToString())}");
                            break;
                        case "scope":
                            parameters.Add($"{propName}={UtilsText.ConvertToEnum<ScopeEnum>(propValue.ToString())}");
                            break;
                        default:
                            parameters.Add($"{propName}={propValue}");
                            break;
                    }
                }
            }

            if (!string.IsNullOrEmpty(startDate))
            {
                parameters.Add($"publishedDate={startDate}");
            }

            if (!string.IsNullOrEmpty(endDate))
            {
                parameters.Add($"publishedDate={endDate}");
            }

            Parameters = String.Join("&", parameters);


        }
        private string BuildUri(string path) =>
        $"{UriPath}/{path}".TrimEnd('/');

        private string BuiltUrlExtesion(string urlExtension) =>
         string.IsNullOrEmpty(Parameters) ? $"{urlExtension}" : $"{urlExtension}?{Parameters}";

    }
}

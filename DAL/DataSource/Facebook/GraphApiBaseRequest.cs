using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Facebook;

public abstract class GraphApiBaseRequest
{
    protected static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    public string EndpointUri { get; set; }
    public string URLExtension { get; set; }
    public string ApiKey { get; set; }
    public string JobLogGuid { get; set; }
    public HttpMethod MethodType { get; set; } = HttpMethod.Get;
    public abstract string UriPath { get; }
    public abstract string HttpBody { get; }
    public string AccountID { get; set; }
    private readonly IHttpClientProvider _httpClientProvider;

    protected GraphApiBaseRequest(IHttpClientProvider httpClientProvider)
    {
        _httpClientProvider = httpClientProvider;
    }

    public virtual async Task<T> FetchDataAsync<T>()
    {
        T deserializedResponse = default;

        HttpRequestOptions requestOptions = new()
        {
            Uri = BuildUri(UriPath),
            AuthToken = ApiKey,
            ContentType = "application/json",
            Method = MethodType,
            Content = new StringContent(HttpBody, Encoding.UTF8, "application/json")
        };

        try
        {
            if (MethodType == HttpMethod.Post)
            {
                return await _httpClientProvider.SendRequestAndDeserializeAsync<T>(requestOptions);
            }

            Dictionary<string, string> headers = new();
            HttpResponseMessage response = await _httpClientProvider.GetResponseAsync(requestOptions);

            foreach (var header in response.Headers)
            {
                headers.Add(header.Key, header.Value.ToString());
            }

            string stringResponse = await response.Content.ReadAsStringAsync();
            deserializedResponse = JsonConvert.DeserializeObject<T>(stringResponse);

            deserializedResponse.GetType().GetProperty("RawJson")
                ?.SetValue(deserializedResponse, stringResponse, null);
            deserializedResponse.GetType().GetProperty("Header")?.SetValue(deserializedResponse, headers, null);
            deserializedResponse.GetType().GetProperty("ResponseCode")
                ?.SetValue(deserializedResponse, response.StatusCode, null);
        }
        catch (HttpClientProviderRequestException e)
        {
            HandleException(requestOptions, e);
            throw;
        }

        return deserializedResponse;
    }

    private void HandleException(HttpRequestOptions requestOptions, HttpClientProviderRequestException e)
    {
        e.Data?.Add("apiException", e.Message);

        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            $"{JobLogGuid} - URL: {SanitizeUrl(requestOptions.Uri)};" +
            $"Body:{HttpBody}-> Exception details : {e}",
            e));
    }

    private string SanitizeUrl(string urlPath)
    {
        var clearnUrl = urlPath.Replace(ApiKey, "<api key sanitized>");
        return clearnUrl;
    }

    private string BuildUri(string path) =>
        $"{EndpointUri}/{path}".TrimEnd('/');
}

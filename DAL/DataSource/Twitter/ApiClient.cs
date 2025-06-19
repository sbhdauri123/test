using Greenhouse.Auth;
using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Twitter;

public class ApiClient : IApiClient
{
    private readonly ApiClientOptions _options;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly IOAuthAuthenticator _oAuthAuthenticator;

    public ApiClient(ApiClientOptions options, IHttpClientProvider httpClientProvider,
        IOAuthAuthenticator oAuthAuthenticator)
    {
        ArgumentNullException.ThrowIfNull(httpClientProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(oAuthAuthenticator);

        options.Validate();

        _options = options;
        _httpClientProvider = httpClientProvider;
        _oAuthAuthenticator = oAuthAuthenticator;
    }

    public async Task<ActiveEntities> GetActiveEntitiesAsync(GetActiveEntitiesOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        Dictionary<string, string> parameters = GetActiveEntitiesParameters(options);
        string endpointUri = BuildUri($"stats/accounts/{options.AccountId}/active_entities");

        HttpRequestOptions request =
            BuildHttpRequestMessage(HttpMethod.Get, endpointUri, parameters, "application/json");

        string response = await _httpClientProvider.SendRequestAsync(request);
        return JsonConvert.DeserializeObject<ActiveEntities>(response);
    }

    public async Task<ReportRequestStatusResponse> GetReportRequestStatusAsync(GetReportRequestStatusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        Dictionary<string, string> parameters = GetReportStatusParameters(options);
        string endpointUri = BuildUri(path: $"stats/jobs/accounts/{options.AccountId}");

        HttpRequestOptions request =
            BuildHttpRequestMessage(HttpMethod.Get, endpointUri, parameters, "application/json");

        string response = await _httpClientProvider.SendRequestAsync(request);
        return JsonConvert.DeserializeObject<ReportRequestStatusResponse>(response);
    }

    public async Task<ReportRequestResponse> GetFactReportAsync(GetFactReportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        Dictionary<string, string> parameters = GetFactReportParameters(options);
        string endpointUri = BuildUri(path: $"{options.ReportType}/jobs/accounts/{options.AccountId}");

        HttpRequestOptions request =
            BuildHttpRequestMessage(HttpMethod.Post, endpointUri, parameters, "application/x-www-form-urlencoded");

        string response = await _httpClientProvider.SendRequestAsync(request);
        return JsonConvert.DeserializeObject<ReportRequestResponse>(response);
    }

    public async Task<Stream> DownloadDimensionFileAsync(DownloadDimensionFileOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        Dictionary<string, string> parameters = GetDimensionParameters(options);
        string accountId = !string.IsNullOrEmpty(options.Report.ReportSettings.Endpoint)
            ? options.AccountId
            : string.Empty;

        string endpointUri = BuildUri(path: $"accounts/{accountId}/{options.Report.ReportSettings.Endpoint}");

        HttpRequestOptions request =
            BuildHttpRequestMessage(HttpMethod.Get, endpointUri, parameters, "application/json");
        return await _httpClientProvider.DownloadFileStreamAsync(request);
    }

    public async Task<Stream> DownloadReportFileAsync(DownloadReportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        const string contentType = "application/json";
        HttpRequestMessage request = _httpClientProvider.BuildRequestMessage(new HttpRequestOptions
        {
            Uri = options.ReportUrl,
            ContentType = contentType,
            Method = HttpMethod.Get,
            Content = new StringContent(string.Empty, Encoding.UTF8, contentType),
            Headers = new Dictionary<string, string> { { "Accept", "*/*" }, }
        });

        return await _httpClientProvider.DownloadFileStreamAsync(request);
    }

    private HttpRequestOptions BuildHttpRequestMessage(HttpMethod httpMethod, string uri,
        Dictionary<string, string> queryParameters, string contentType)
    {
        return new HttpRequestOptions
        {
            Uri = $"{uri}?{EncodeQueryString(queryParameters)}",
            ContentType = contentType,
            Content = new StringContent(string.Empty, Encoding.UTF8, contentType),
            Method = httpMethod,
            Headers = new Dictionary<string, string>
            {
                { "Accept", "*/*" },
                {
                    "Authorization", _oAuthAuthenticator.GenerateOAuthHeader(uri,
                        GetQueryString(queryParameters),
                        httpMethod)
                }
            }
        };
    }

    private static string EncodeQueryString(Dictionary<string, string> parameters)
    {
        if (parameters == null || parameters.Count == 0)
        {
            return string.Empty;
        }

        return string.Join("&", parameters
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));
    }

    private static string GetQueryString(Dictionary<string, string> parameters)
        => string.Join("&", parameters.OrderBy(kvp => kvp.Key).Select(kvp => $"{kvp.Key}={kvp.Value}"));

    private static string FormatDateTime(DateTime dateTime, string granularity) =>
        dateTime.ToString(granularity == "DAY" ? "yyyy-MM-dd" : "yyyy-MM-ddTHH:mm:ssZ");

    private string BuildUri(string path) =>
        $"{_options.EndpointUri.TrimEnd('/')}/{_options.Version}/{path}".TrimEnd('/');

    private static Dictionary<string, string> GetActiveEntitiesParameters(GetActiveEntitiesOptions options)
    {
        return new Dictionary<string, string>()
        {
            ["entity"] = options.Entity,
            ["start_time"] = FormatDateTime(options.FileDate, options.Granularity),
            ["end_time"] = FormatDateTime(options.FileDate.AddDays(1), options.Granularity),
        };
    }

    private static Dictionary<string, string> GetReportStatusParameters(GetReportRequestStatusOptions options)
        => new() { ["job_ids"] = string.Join(",", options.JobIds) };

    private static Dictionary<string, string> GetFactReportParameters(GetFactReportOptions options)
    {
        Dictionary<string, string> parameters = new()
        {
            ["entity"] = options.Entity,
            ["entity_ids"] = string.Join(",", options.EntityIds),
            ["start_time"] = FormatDateTime(options.FileDate, options.Granularity),
            ["end_time"] = FormatDateTime(options.FileDate.AddDays(1), options.Granularity),
            ["granularity"] = options.Granularity,
            ["placement"] = options.Placement,
            ["metric_groups"] = options.MetricGroups
        };

        if (!string.IsNullOrEmpty(options.Segmentation))
        {
            parameters["segmentation_type"] = options.Segmentation;
        }

        if (!string.IsNullOrEmpty(options.SegmentationType) && !string.IsNullOrEmpty(options.SegmentationValue))
        {
            parameters[options.SegmentationType] = options.SegmentationValue;
        }

        return parameters;
    }

    private static Dictionary<string, string> GetDimensionParameters(DownloadDimensionFileOptions options)
    {
        Dictionary<string, string> parameters = new();

        string entityIds = string.Join(",", options.EntityIds);
        APIReport<ReportSettings> report = options.Report;

        if (!string.IsNullOrEmpty(options.Cursor))
        {
            parameters["cursor"] = options.Cursor;
        }

        if (report.ReportSettings.WithDeleted)
        {
            parameters["with_deleted"] = report.ReportSettings.WithDeleted.ToString().ToLower();
        }

        if (report.ReportSettings.IncludeLegacyCards)
        {
            parameters["include_legacy_cards"] = report.ReportSettings.IncludeLegacyCards.ToString().ToLower();
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.TweetType))
        {
            parameters["tweet_type"] = report.ReportSettings.TweetType;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.TimelineType))
        {
            parameters["timeline_type"] = report.ReportSettings.TimelineType;
        }

        if (!string.IsNullOrEmpty(entityIds) && !string.IsNullOrEmpty(report.ReportSettings.EntityIdsParamName))
        {
            parameters[report.ReportSettings.EntityIdsParamName] = entityIds;
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.PageSize))
        {
            parameters["count"] = report.ReportSettings.PageSize;
        }

        return parameters;
    }
}
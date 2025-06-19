using Greenhouse.Auth;
using Greenhouse.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.LinkedIn;

public class ApiClient : IApiClient
{
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly ITokenApiClient _tokenApiClient;
    private readonly ApiClientOptions _options;

    private const string ContentType = "application/x-www-form-urlencoded";
    private const string AuthScheme = "Bearer";

    private readonly Dictionary<string, string> _headers = new()
    {
        { "X-Restli-Protocol-Version", "2.0.0" },
        { "Linkedin-Version", "202404" },
        { "X-HTTP-Method-Override", "GET" },
        { "User-Agent", "Publicis" }
    };

    public ApiClient(ApiClientOptions options, IHttpClientProvider httpClientProvider, ITokenApiClient tokenApiClient)
    {
        ArgumentNullException.ThrowIfNull(httpClientProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(tokenApiClient);

        options.Validate();

        _httpClientProvider = httpClientProvider;
        _tokenApiClient = tokenApiClient;
        _options = options;
    }

    public async Task<Stream> DownloadFactReportStreamAsync(FactReportDownloadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        string endpointUri = BuildFactReportUri(options);
        string requestBody = BuildFactReportRequestBody(options);
        HttpRequestOptions requestOptions = await BuildRequestOptions(endpointUri, requestBody);
        return await _httpClientProvider.DownloadFileStreamAsync(requestOptions);
    }

    public async Task<Stream> DownloadAdAccountsReportStreamAsync(AdAccountsReportDownloadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        string endpointUri = BuildAdAccountsReportUri(options);
        HttpRequestOptions requestOptions = await BuildRequestOptions(endpointUri);
        return await _httpClientProvider.DownloadFileStreamAsync(requestOptions);
    }

    public async Task<Stream> DownloadAdCampaignGroupsReportStreamAsync(DimensionReportDownloadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        string endpointUri = BuildDimensionReportUri(options);
        string requestBody = BuildCampaignGroupReportRequestBody(options);

        return await DownloadDimensionReportStreamAsync(endpointUri, requestBody);
    }

    public async Task<Stream> DownloadAdCampaignsReportStreamAsync(DimensionReportDownloadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        string endpointUri = BuildDimensionReportUri(options);
        string requestBody = BuildCampaignReportRequestBody(options);

        return await DownloadDimensionReportStreamAsync(endpointUri, requestBody);
    }

    public async Task<Stream> DownloadCreativesReportStreamAsync(DimensionReportDownloadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();

        string endpointUri = BuildDimensionReportUri(options);
        string requestBody = BuildCreativesReportRequestBody(options);

        return await DownloadDimensionReportStreamAsync(endpointUri, requestBody);
    }

    private async Task<Stream> DownloadDimensionReportStreamAsync(string endpointUri, string requestBody)
    {
        HttpRequestOptions requestOptions = await BuildRequestOptions(endpointUri, requestBody);
        return await _httpClientProvider.DownloadFileStreamAsync(requestOptions);
    }

    private async Task<HttpRequestOptions> BuildRequestOptions(string endpointUri, string requestBody = null)
    {
        string accessToken = await _tokenApiClient.GetAccessTokenAsync(TokenDataSource.LinkedIn);

        return new HttpRequestOptions
        {
            Uri = endpointUri,
            AuthToken = accessToken,
            AuthScheme = AuthScheme,
            ContentType = ContentType,
            Method = HttpMethod.Post,
            Content = new StringContent(requestBody ?? string.Empty, Encoding.UTF8, ContentType),
            Headers = _headers
        };
    }

    private string BuildCampaignGroupReportRequestBody(DimensionReportDownloadOptions options)
    {
        StringBuilder stringBuilder = new("q=search&search=(test:false)");
        stringBuilder.Append(BuildDimensionReportRequestBody(options));

        return stringBuilder.ToString();
    }

    private string BuildCampaignReportRequestBody(DimensionReportDownloadOptions options)
    {
        StringBuilder stringBuilder = new();

        stringBuilder.Append(BuildSearchString(
            baseStringValue: "q=search&search=(test:false,campaignGroup:(values:List(",
            searchUrnValue: "sponsoredCampaignGroup",
            closingStringValue: ")))",
            searchIds: options.SearchIds));

        stringBuilder.Append(BuildDimensionReportRequestBody(options));

        return stringBuilder.ToString();
    }

    private string BuildCreativesReportRequestBody(DimensionReportDownloadOptions options)
    {
        StringBuilder stringBuilder = new();

        stringBuilder.Append(BuildSearchString(
            baseStringValue: "q=criteria&isTestAccount=false&campaigns=List(",
            searchUrnValue: "sponsoredCampaign",
            closingStringValue: ")",
            searchIds: options.SearchIds));

        stringBuilder.Append(BuildDimensionReportRequestBody(options));

        return stringBuilder.ToString();
    }

    private static string BuildSearchString(
        string baseStringValue,
        string searchUrnValue,
        string closingStringValue,
        IEnumerable<string> searchIds)
    {
        StringBuilder stringBuilder = new(baseStringValue);
        bool addedId = false;

        foreach (string searchId in searchIds)
        {
            string baseSearchString = $"urn%3Ali%3A{searchUrnValue}%3A{searchId}";
            string searchString = addedId ? ("," + baseSearchString) : baseSearchString;
            stringBuilder.Append(searchString);
            addedId = true;
        }

        stringBuilder.Append(closingStringValue);

        return stringBuilder.ToString();
    }

    private string BuildDimensionReportRequestBody(DimensionReportDownloadOptions options)
        => $"&pageSize={_options.PageSize}"
           + (string.IsNullOrEmpty(options.NextPageToken)
               ? ""
               : $"&pageToken={options.NextPageToken}");

    private string BuildDimensionReportUri(DimensionReportDownloadOptions options)
        => $"{_options.EndpointUri.TrimEnd('/')}/adAccounts/{options.AccountId}/{options.DeliveryPath}";

    private string BuildAdAccountsReportUri(AdAccountsReportDownloadOptions options)
        => $"{_options.EndpointUri.TrimEnd('/')}/adAccounts/{options.AccountId}";

    private string BuildFactReportUri(FactReportDownloadOptions options)
        => $"{_options.EndpointUri.TrimEnd('/')}/{options.DeliveryPath}";

    private static string BuildFactReportRequestBody(FactReportDownloadOptions options)
        => $"accounts=List(urn%3Ali%3AsponsoredAccount%3A{options.AccountId})"
           + $"&dateRange=(start:(day:{options.FileDate.Day},month:{options.FileDate.Month},year:{options.FileDate.Year}),"
           + $"end:(day:{options.FileDate.Day},month:{options.FileDate.Month},year:{options.FileDate.Year}))"
           + $"&pivot=CREATIVE&q=analytics&timeGranularity=DAILY&fields={string.Join(",", options.ReportFieldNames)}";
}
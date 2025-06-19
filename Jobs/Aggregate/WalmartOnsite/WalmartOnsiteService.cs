using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.WalmartOnsite;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.WalmartOnsite;

public class WalmartOnsiteService
{
    private readonly string _consumerId;
    private readonly int _keyVersion;
    private readonly string _privateKey;
    private readonly string _authToken;
    private readonly string _integrationEndpointURI;

    private long _inTimeStamp;
    private string _authSignature;

    private readonly IHttpClientProvider _httpClientProvider;
    private readonly RSA _rsa;
    private readonly JsonSerializerSettings _jsonSerializerSettings;

    //Timestamp is technically valid for 5 mins, but we are giving a one minute buffer
    private int VALID_TIMESTAMP_DURATION_IN_MINS = 4;

    public WalmartOnsiteService(WalmartOnsiteServiceOptions options, IHttpClientProvider httpClientProvider)
    {
        ArgumentNullException.ThrowIfNull(options);

        _consumerId = options.ConsumerId;
        _keyVersion = options.KeyVersion;
        _privateKey = options.PrivateKey;
        _authToken = options.AuthToken;
        _integrationEndpointURI = options.IntegrationEndpointURI.TrimEnd('/');
        _httpClientProvider = httpClientProvider;

        _rsa = RSA.Create();
        _rsa.ImportFromPem(_privateKey.ToCharArray());

        _jsonSerializerSettings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore
        };
    }

    private string GetIntTimeStamp()
    {
        var currentTicks = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var tickDifference = TimeSpan.FromMilliseconds(currentTicks - _inTimeStamp);
        double minutes = tickDifference.TotalMinutes;

        if (_inTimeStamp == 0 || minutes > VALID_TIMESTAMP_DURATION_IN_MINS)
        {
            _inTimeStamp = currentTicks;
            _authSignature = null;
        }

        return _inTimeStamp.ToString();
    }

    private string GetAuthSignature(string timestamp)
    {
        if (string.IsNullOrEmpty(timestamp))
        {
            throw new ArgumentException("Cannot generate an auth signature if timestamp is null or empty.");
        }

        if (!string.IsNullOrEmpty(_authSignature))
        {
            return _authSignature;
        }

        var stringToEncrypt = $"{_consumerId}\n{timestamp}\n{_keyVersion}\n";
        var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(stringToEncrypt);

        var signBytes = _rsa.SignData(plainTextBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        _authSignature = Convert.ToBase64String(signBytes);

        return _authSignature;
    }

    private Dictionary<string, string> GenerateHeaders()
    {
        Dictionary<string, string> headers = new Dictionary<string, string>();

        var timestamp = GetIntTimeStamp();
        var authSignature = GetAuthSignature(timestamp);

        headers.Add("WM_SEC.AUTH_SIGNATURE", authSignature);
        headers.Add("WM_SEC.KEY_VERSION", _keyVersion.ToString());
        headers.Add("WM_CONSUMER.ID", _consumerId.ToString());
        headers.Add("WM_CONSUMER.intimestamp", timestamp);

        return headers;
    }

    public async Task GenerateSnapshotAsync(ApiReportItem apiReportItem)
    {
        HttpResponseMessage response = await _httpClientProvider.GetResponseAsync(new HttpRequestOptions
        {
            Uri = $"{_integrationEndpointURI}/api/{apiReportItem.Version}/snapshot/{apiReportItem.ReportType.ToString().ToLower()}",
            AuthToken = _authToken,
            Content = new StringContent(GetSnapshotRequestBody(apiReportItem), Encoding.UTF8, "application/json"),
            ContentType = "application/json",
            Method = HttpMethod.Post,
            Headers = GenerateHeaders()
        });

        string stringResponse = await response.Content.ReadAsStringAsync();
        JsonSerializerSettings settings = new()
        {
            NullValueHandling = NullValueHandling.Ignore,
            MissingMemberHandling = MissingMemberHandling.Ignore
        };
        SnapshotReportReponse deserializedResponse =
            JsonConvert.DeserializeObject<SnapshotReportReponse>(stringResponse, settings);

        if (deserializedResponse.SnapShotId == string.Empty)
        {
            apiReportItem.IsFailed = true;
            return;
        }

        apiReportItem.SnapShotID = deserializedResponse.SnapShotId;
    }

    private string GetSnapshotRequestBody(ApiReportItem apiReportItem)
    {
        ArgumentNullException.ThrowIfNull(apiReportItem);

        var fileDate = apiReportItem.ReportDate.ToString("yyyy-MM-dd");
        var reportMetrics = apiReportItem.ReportFields.Select(x => x.APIReportFieldName).ToList();

        if (apiReportItem.ReportType == ReportType.Report)
        {
            ReportSnapshotRequestBody snapshotReportBody = apiReportItem.SnapShotEntity switch
            {
                "outOfBudgetRecommendations" or "searchImpression" => new ReportSnapshotRequestBody
                {
                    AdvertiserId = apiReportItem.AdvertiserID,
                    ReportType = apiReportItem.SnapShotEntity,
                    ReportMetrics = reportMetrics
                },
                "itemKeyword" => new ReportSnapshotRequestBody
                {
                    AdvertiserId = apiReportItem.AdvertiserID,
                    ReportType = apiReportItem.SnapShotEntity,
                    StartDate = fileDate,
                    EndDate = fileDate,
                    AttributionWindow = apiReportItem.AttributionWindow,
                    Format = apiReportItem.FileExtension
                },
                _ => new ReportSnapshotRequestBody
                {
                    AdvertiserId = apiReportItem.AdvertiserID,
                    StartDate = fileDate,
                    EndDate = fileDate,
                    ReportType = apiReportItem.SnapShotEntity,
                    ReportMetrics = reportMetrics
                }
            };

            return JsonConvert.SerializeObject(snapshotReportBody, _jsonSerializerSettings);
        }

        if (apiReportItem.ReportType == ReportType.Entity)
        {
            var entitySnapshotBody = new EntitySnapshotRequestBody
            {
                AdvertiserId = apiReportItem.AdvertiserID,
                EntityStatus = apiReportItem.EntityStatus.ToString().ToLower(),
                EntityTypes = apiReportItem.EntityTypes,
                Format = apiReportItem.FileExtension
            };

            return JsonConvert.SerializeObject(entitySnapshotBody, _jsonSerializerSettings);
        }

        throw new APIReportException($"ReportType: {apiReportItem.ReportType} not recognized.");
    }

    public async Task<SnapshotReportReponse> GetSnapshotStatus(ApiReportItem apiReportItem)
    {
        var headers = GenerateHeaders();

        var httpRequestMessageSettings = new HttpRequestOptions
        {
            Uri = $"{_integrationEndpointURI}/api/{apiReportItem.Version}/snapshot?advertiserId={apiReportItem.AdvertiserID}&snapshotId={apiReportItem.SnapShotID}",
            AuthToken = _authToken,
            ContentType = "application/json",
            Method = HttpMethod.Get,
            Headers = headers
        };

        var httpRequest = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

        return await _httpClientProvider.SendRequestAndDeserializeAsync<SnapshotReportReponse>(httpRequest);
    }

    public async Task<Stream> DownloadReportsAsync(ApiReportItem apiReportItem)
    {
        Dictionary<string, string> headers = GenerateHeaders();

        SnapshotDownloadRequestBody snapshotReportBody = new()
        {
            AdvertiserId = apiReportItem.AdvertiserID,
            SnapShotId = apiReportItem.SnapShotID
        };

        string httpBody = JsonConvert.SerializeObject(snapshotReportBody,
            new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

        return await _httpClientProvider.DownloadFileStreamAsync(new HttpRequestOptions
        {
            Uri = apiReportItem.DownloadURI.Contains("display") ? $"{apiReportItem.DownloadURI}?advertiserId={apiReportItem.AdvertiserID}" : apiReportItem.DownloadURI,
            AuthToken = _authToken,
            ContentType = MediaTypeNames.Application.Json,
            Method = HttpMethod.Get,
            Headers = headers
        });
    }
}

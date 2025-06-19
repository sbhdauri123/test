using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.TikTok;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.TikTok;

public class TikTokService
{
    private readonly string _hostURI;
    private readonly string _version;
    private readonly string _token;
    private readonly IHttpClientProvider _httpClientProvider;
    private readonly ParallelOptions _apiParallelOptions;

    private readonly int REQUEST_TOO_FREQUENT_CODE = 40100;
    private const int SUCCESSFUL_REQUEST_CODE = 0;
    private readonly int THREAD_SLEEP;
    private readonly int ID_REQUEST_LIMIT = 100;
    private readonly int PAGE_SIZE;

    private Dictionary<string, List<string>> _advertiserIdAndAdIdPairs = new();

    public TikTokService(
        TikTokServiceOptions options,
        IHttpClientProvider httpClientProvider)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));

        options.Verify();

        _hostURI = options.HostURI;
        _version = options.Version;
        _token = options.Token;
        THREAD_SLEEP = options.ThreadSleep;
        _apiParallelOptions = options.ParallelOptions;
        PAGE_SIZE = options.PageSize;

        _httpClientProvider = httpClientProvider;
    }

    public async Task DownloadReportAsync(ApiReportRequest options, Action<string> threadSleepAction, Action<Stream, int> saveFileAction)
    {
        ArgumentNullException.ThrowIfNull(options, nameof(options));

        if (options.IsTask)
        {
            HttpRequestMessage request = GetRequest(options, HttpMethod.Get);
            await using Stream streamResponse = await _httpClientProvider.DownloadFileStreamAsync(request);
            saveFileAction(streamResponse, 0);
            return;
        }

        int currentPage = 1;
        int totalPages;

        do
        {
            HttpRequestMessage request = GetRequest(options, HttpMethod.Get);
            await using Stream streamResponse = await _httpClientProvider.DownloadFileStreamAsync(request);
            using StreamReader streamReader = new(streamResponse);

            string content = await streamReader.ReadToEndAsync();
            ReportResponse response = JsonConvert.DeserializeObject<ReportResponse>(content) ??
                                      throw new APIResponseException("Failed to deserialize response from API.");

            totalPages = response.ReportData?.PageInfo?.TotalPage ?? 0;
            bool successfulResponseCode = await CheckResponseCodeAsync(response.Code, threadSleepAction);
            if (!successfulResponseCode)
            {
                throw new APIResponseException($"{response.Code} - {JsonConvert.SerializeObject(response)}");
            }

            saveFileAction(streamResponse, currentPage);

            currentPage++;

            if (totalPages > 1)
            {
                options.CurrentPage = currentPage;
            }

        } while (currentPage <= totalPages);
    }

    public async Task DownloadFactReportAsync(ApiReportRequest downloadRequest, Action<string> threadSleepAction, Action<Stream, string, int> saveFileAction)
    {
        //Get all of the ads for the Fact Reports
        List<string> campaignIds = await GetCampaignIdsForAdvertiserIdAsync(
            downloadRequest.ProfileID,
            threadSleepAction: threadSleepAction);


        if (campaignIds.Count == 0)
        {
            //Donwload the report, which will have an empty list
            downloadRequest.SetParameters();

            await DownloadReportAsync(downloadRequest,
                threadSleepAction: threadSleepAction,
                saveFileAction: (stream, pageNumber) => saveFileAction(stream, "0", pageNumber));

            return;
        }

        ConcurrentQueue<Exception> exceptions = new();

        var subLists = campaignIds.Chunk(ID_REQUEST_LIMIT);

        await Parallel.ForEachAsync(subLists, _apiParallelOptions, async (list, _) =>
        {
            //We generate a new request for each thread to prevent threads from overwriting each others' lists
            var duplicatedApiReportRequest = GenerateApiReportRequestWithAds(downloadRequest, list.ToList());
            duplicatedApiReportRequest.SetParameters();

            //Use the first campaignId in the list to differentiate it from other threads' downloads
            var firstCampaignId = list[0];

            try
            {
                await DownloadReportAsync(duplicatedApiReportRequest,
                    threadSleepAction: threadSleepAction,
                    saveFileAction: (stream, pageNumber) =>
                    {
                        saveFileAction(stream, firstCampaignId, pageNumber);
                    });
            }
            catch (Exception ex)
            {
                exceptions.Enqueue(ex);
            }

        });//end of foreach

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

    }

    public async Task<List<string>> GetCampaignIdsForAdvertiserIdAsync(string advertiserId, Action<string> threadSleepAction)
    {
        //First, check the cache to see if we already have it for the advertiserId
        if (_advertiserIdAndAdIdPairs.TryGetValue(advertiserId, out List<string> cachedAdIds))
        {
            return cachedAdIds;
        }

        int currentPage = 1;
        int totalPages = 1;

        List<string> campaignIds = new();

        TikTokDownloadReportOptions options = new()
        {
            Endpoint = "campaign/get/",
            QueryParameters = new()
                {
                    { "advertiser_id", advertiserId },
                    { "page_size", PAGE_SIZE.ToString() },
                    { "fields", "[\"campaign_id\"]"},
                    { "page", "1" }
                }
        };

        while (currentPage <= totalPages)
        {
            HttpRequestMessage request = GetRequest(options);
            await using Stream campaignStream = await _httpClientProvider.DownloadFileStreamAsync(request);
            using StreamReader streamReader = new(campaignStream);
            string content = await streamReader.ReadToEndAsync();

            var campaignReportResponse = JsonConvert.DeserializeObject<CampaignReportResponse>(content);

            var successfulResponseCode = await CheckResponseCodeAsync(campaignReportResponse.Code, threadSleepAction);

            if (!successfulResponseCode)
            {
                throw new APIResponseException($"{campaignReportResponse.Code} - {JsonConvert.SerializeObject(campaignReportResponse)}");
            }

            campaignIds.AddRange(campaignReportResponse.CampaignReportList.CampaignIds.Select(x => x.CampaignId));

            currentPage++;
            totalPages = campaignReportResponse.PageInfo?.TotalPage ?? 0;

            if (totalPages > 1)
            {
                options.QueryParameters["page"] = currentPage.ToString();
            }
        }

        if (campaignIds.Count != 0)
        {
            _advertiserIdAndAdIdPairs.Add(advertiserId, campaignIds);
        }

        return campaignIds;
    }

    #region Tiktok Async API
    public async Task<string> SubmitReportAsync(ApiReportRequest request, Action<HttpRequestMessage> logCallback)
    {
        var httpRequest = GetRequest(request, HttpMethod.Post);

        logCallback(httpRequest);

        var response = await _httpClientProvider.SendRequestAndDeserializeAsync<SubmitReportResponse>(httpRequest);

        if (response.Code != 0)
        {
            throw new APIResponseException($"Response code for submitting report to async API was not successful. Code: {response.Code} -> Request URI: {httpRequest.RequestUri} -> Response Message: {response.Message}");
        }

        return response.Data.TaskId;
    }

    public async Task<string> CheckReportStatus(ApiReportRequest request)
    {
        var httpRequest = GetRequest(request, HttpMethod.Get);

        var response = await _httpClientProvider.SendRequestAndDeserializeAsync<CheckStatusResponse>(httpRequest);

        return response.Data.Status;
    }

    #endregion

    private async Task<bool> CheckResponseCodeAsync(int code, Action<string> threadSleepAction)
    {
        // If on list of ones that are critical failures then stop job to fail fast, ie max call limit
        // If code for too many consecutive calls then slow job down (40100 Request too frequent)
        if (code != REQUEST_TOO_FREQUENT_CODE)
        {
            return code == SUCCESSFUL_REQUEST_CODE;
        }

        await ThreadSleepAsync(threadSleepAction);
        return false;
    }

    private async Task ThreadSleepAsync(Action<string> threadSleepAction)
    {
        var milliseconds = 1000 * THREAD_SLEEP;

        threadSleepAction($"Putting thread to sleep for {THREAD_SLEEP} second(s) before next request.");

        await Task.Run(async () => await Task.Delay(milliseconds));
        threadSleepAction($"Thread sleep for {THREAD_SLEEP} second(s) is Complete");
    }

    private HttpRequestMessage GetRequest(ApiReportRequest options, HttpMethod method)
    {
        Dictionary<string, string> headers = new()
        {
            { "Access-Token", _token }
        };

        options.SetParameters();

        var settings = new HttpRequestOptions
        {
            Uri = options.UriPath,
            Method = method,
            ContentType = "application/json",
            Headers = headers
        };

        return _httpClientProvider.BuildRequestMessage(settings);
    }

    private HttpRequestMessage GetRequest(TikTokDownloadReportOptions options)
    {
        var uri = $"{_hostURI.TrimEnd('/')}/{_version}/{options.Endpoint}?";
        uri += string.Join("&", options.QueryParameters.Select(x => $"{x.Key}={x.Value}"));

        Dictionary<string, string> headers = new()
        {
            { "Access-Token", _token }
        };

        var settings = new HttpRequestOptions
        {
            Uri = uri,
            Method = HttpMethod.Get,
            ContentType = "application/json",
            Headers = headers
        };

        return _httpClientProvider.BuildRequestMessage(settings);
    }

    private static ApiReportRequest GenerateApiReportRequestWithAds(ApiReportRequest apiReportRequest, List<string> campaignIds)
    {
        return new ApiReportRequest
        {
            ProfileID = apiReportRequest.ProfileID,
            MethodType = System.Net.Http.HttpMethod.Get.ToString(),
            ReportPath = apiReportRequest.ReportPath,
            IsAccountInfo = apiReportRequest.IsAccountInfo,
            StartDate = apiReportRequest.StartDate,
            EndDate = apiReportRequest.EndDate,
            ReportSettings = apiReportRequest.ReportSettings,
            PageSize = apiReportRequest.PageSize,
            Metrics = apiReportRequest.Metrics,
            Dimensions = apiReportRequest.Dimensions,
            CampaignIds = campaignIds
        };
    }
}

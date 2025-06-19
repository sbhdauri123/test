using Greenhouse.Common.Infrastructure;
using Greenhouse.Data.DataSource.Snapchat;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.DAL.DataSource.Snapchat;

public class SnapchatProvider
{
    private SnapchatOAuth OAuth { get; }
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private string JobGuid { get; }
    protected Credential GreenhouseS3Creds { get; set; }
    private readonly IHttpClientProvider _httpClientProvider;

    public SnapchatProvider(IHttpClientProvider httpClientProvider, Greenhouse.Data.Model.Setup.Credential creds, Credential s3Creds, string jobGuid)
    {
        _httpClientProvider = httpClientProvider;
        this.OAuth = new SnapchatOAuth(creds, _httpClientProvider);
        this.GreenhouseS3Creds = s3Creds;
        this.JobGuid = jobGuid;
    }

    public List<ApiReportItem> GetSnapchatReport(Queue queueItem, APIReport<ReportSettings> report, ApiReportRequest reportRequest
        , Dictionary<string, List<string>> parentCache, IRetry retry, Action<Stream, string> saveFileAction, List<Mapping> mappingFile = null)
    {
        var reports = new List<ApiReportItem>();
        var nextPageLink = reportRequest.GetFullUriPathWithParameters();
        int counter = 0;
        var nextLinks = new Queue<string>();

        while (!string.IsNullOrEmpty(nextPageLink))
        {
            var currReport = new ApiReportItem(queueItem, report, reportRequest, nextPageLink, counter);

            //stream data to local file since we will need to stage data later on
            //previously streamed to s3, but we are trying to keep costs down
            (string requestID, JObject contentObject) = retry.Execute(() => StreamToS3(reportRequest, currReport, nextLinks, (stream) => saveFileAction(stream, currReport.FileName)).Result);

            // map additional info (profileID) to the requestID from the raw file
            // this will be saved in a file so etl can associate the additional info
            // to a file
            mappingFile?.Add(new Mapping { RequestID = requestID, AccountID = currReport.ProfileID });

            // if the report has a value set for the property ParentDataPath
            // this means that some data needs to be extracted to be used
            // for other reports that have that Report Entity as parent
            // ParentDataPath is the json path to the data to cache
            if (!string.IsNullOrEmpty(report.ReportSettings.ParentDataPath))
            {
                var data = contentObject.SelectTokens(report.ReportSettings.ParentDataPath).Select(d => d.ToString()).ToList();
                string key = report.ReportSettings.Entity.ToLower();

                if (!parentCache.TryAdd(key, data))
                    parentCache[key].AddRange(data);
            }

            reports.Add(currReport);
            counter++;
            nextPageLink = nextLinks.Count > 0 ? nextLinks.Dequeue() : null;
        }

        return reports;
    }

    private async Task<(string requestID, JObject contentObject)> StreamToS3(ApiReportRequest reportRequest, ApiReportItem currReport, Queue<string> links
        , Action<Stream> saveFileAction)
    {
        string requestID;
        JObject contentObject;
        List<string> newLinks = new();

        reportRequest.AuthToken = await this.OAuth.GetSnapChatAccessTokenAsync();

        var httpRequestMessageSettings = new HttpRequestOptions()
        {
            Uri = currReport.ReportURL,
            AuthToken = reportRequest.AuthToken,
            ContentType = "application/json",
            Method = reportRequest.MethodType,
        };

        var request = _httpClientProvider.BuildRequestMessage(httpRequestMessageSettings);

        try
        {
            await using Stream streamResponse = await _httpClientProvider.DownloadFileStreamAsync(request);
            using StreamReader streamReader = new(streamResponse);

            string content = await streamReader.ReadToEndAsync();

            contentObject = JObject.Parse(content);
            requestID = contentObject.SelectToken("$.request_id").Value<string>();

            if (reportRequest.IsDimension)
            {
                var dimensionReport = JsonConvert.DeserializeObject<DimensionReport<object>>(content);

                newLinks.Add(dimensionReport.Paging?.next_page_link);
            }
            else
            {
                StatsReport statsReport = null;

                ///since json schema structure  is identical, using  SerializerSetting to set the right data.
                if (currReport.ReportType.Equals("conversions", StringComparison.InvariantCultureIgnoreCase))
                {
                    statsReport = JsonConvert.DeserializeObject<StatsReport>(content, Data.DataSource.Snapchat.BreakdownConverter.Settings);
                }
                else
                {
                    statsReport = JsonConvert.DeserializeObject<StatsReport>(content);
                }
                var nxtLinks = statsReport.TimeseriesStats.Where(x => !string.IsNullOrEmpty(x.TimeseriesStat.Paging?.next_page_link)).Select(x => x.TimeseriesStat.Paging.next_page_link).ToList();
                newLinks.AddRange(nxtLinks);
            }

            saveFileAction(streamResponse);
        }
        catch (HttpClientProviderRequestException ex)
        {
            //log the entity and request URL before rethrowing web exception to Polly
            logger.Error($"{JobGuid} - HttpClientProviderRequestException Error - failed on queueID: {currReport.QueueID}, " +
                $"FileGUID: {currReport.FileGuid}, " +
                $"URL: {currReport.ReportURL} - Exception details : {ex}");
            throw;
        }

        newLinks.ForEach(link =>
        {
            if (!string.IsNullOrEmpty(link) && !links.Contains(link))
            {
                links.Enqueue(link);
            }
        });

        if (links.Count > 0)
        {
            // delay between requests
            Task.Delay(300).Wait();
        }

        return (requestID, contentObject);
    }
}
[Serializable]
public class Mapping
{
    [JsonProperty("requestID")]
    public string RequestID { get; set; }
    [JsonProperty("accountID")]
    public string AccountID { get; set; }
}

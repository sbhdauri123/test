using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.Skai;
using Greenhouse.Data.DataSource.Skai.AsyncReport;
using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using Polly;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Skai
{
    public class SkaiService
    {
        private const string GROUP_NAME_PREFIX = "groupnamevalue=";
        private readonly Action<LogLevel, string> _logMessage;
        private readonly Action<LogLevel, string, Exception> _logException;
        private readonly SkaiOAuth _skaiOAuth;
        private readonly List<SkaiSavedColumn> _savedColumns;
        private readonly IHttpClientProvider _httpClientProvider;
        private readonly string _endpointUri;
        private readonly ParallelOptions _apiParallelOptions;
        private readonly ResiliencePipeline _rateLimiter;
        private readonly GuardrailConfig _guardrails;

        public SkaiService(SkaiServiceArguments serviceArguments)
        {
            _logMessage = serviceArguments.LogMessage;
            _logException = serviceArguments.LogException;
            _apiParallelOptions = serviceArguments.ParallelOptions;
            _skaiOAuth = serviceArguments.SkaiOAuth;
            _savedColumns = serviceArguments.CustomColumns;
            _httpClientProvider = serviceArguments.HttpClientProvider;
            _endpointUri = serviceArguments.EndpointUri;
            _rateLimiter = serviceArguments.ResiliencePipeline;
            _guardrails = serviceArguments.GuardrailConfig;
        }

        public async Task<Stream> DownloadReportAsync(ApiReportItem reportItem, CancellationToken cancellationToken = default)
        {
            ApiRequest reportRequest = new(_endpointUri, _skaiOAuth, reportItem.ApiEndpoint, _httpClientProvider)
            {
                ServerID = reportItem.ServerID,
                ReportID = reportItem.ReportToken,
                MethodType = System.Net.Http.HttpMethod.Get,
                ContentType = "application/x-zip-compressed"
            };

            reportRequest.SetParameters();

            return await _rateLimiter.ExecuteAsync(async token => await reportRequest.FetchDataAsync(token),
                cancellationToken);
        }

        public async Task<AsyncStatusResponse> CheckReportStatusAsync(ApiReportItem reportItem)
        {
            var reportRequest = new ApiRequest(_endpointUri, _skaiOAuth, reportItem.ApiEndpoint, _httpClientProvider)
            {
                ServerID = reportItem.ServerID,
                ReportID = reportItem.ReportToken,
                IsStatusCheck = true,
                MethodType = System.Net.Http.HttpMethod.Get
            };

            reportRequest.SetParameters();

            var reportResponse = await _rateLimiter.ExecuteAsync(async (_) => await reportRequest.FetchDataAsync<AsyncStatusResponse>());

            _logMessage(LogLevel.Debug, $"Check Report Status: FileGUID: {reportItem.FileGuid}->API Response: {reportItem.ReportToken}->{JsonConvert.SerializeObject(reportResponse)}");

            if (Enum.TryParse(reportResponse.Status, out FusionReportStatus reportStatus))
            {
                reportItem.Status = reportStatus;
            }
            else
            {
                reportItem.Status = FusionReportStatus.UNKNOWN;
            }

            return reportResponse;
        }

        public async Task<ApiReportItem> RequestFusionReportAsync(Queue queueItem, APIReport<ReportSettings> report)
        {
            var reportItem = new ApiReportItem()
            {
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportName = report.APIReportName.ToLower(),
                ServerID = queueItem.EntityID,
                FileExtension = report.ReportSettings?.AsyncRequest?.CompressMethod == "ZIP" ? "zip" : "csv.gz",
                ApiEndpoint = report.ReportSettings.Path,
                ApiReportType = report.ReportSettings.ReportType,
                ApiReportEntity = report.ReportSettings.Entity
            };

            var reportRequest = new ApiRequest(_endpointUri, _skaiOAuth, report.ReportSettings.Path, _httpClientProvider)
            {
                ServerID = queueItem.EntityID,
                MethodType = new System.Net.Http.HttpMethod(report.ReportSettings.Method)
            };

            reportRequest.SetParameters();

            reportRequest.BodyRequestAsync = report.ReportSettings?.AsyncRequest ?? new AsyncRequest();

            reportRequest.BodyRequestAsync.Fields = report.ReportFields?.Select(x => x.APIReportFieldName.StartsWith(GROUP_NAME_PREFIX)
                ? new Greenhouse.Data.DataSource.Skai.AsyncReport.Field
                {
                    Group = ParseApiReportFieldName(x.APIReportFieldName, GROUP_NAME_PREFIX)[0],
                    Name = ParseApiReportFieldName(x.APIReportFieldName, GROUP_NAME_PREFIX)[1]
                }
                : new Greenhouse.Data.DataSource.Skai.AsyncReport.Field { Name = x.APIReportFieldName }
            );

            reportRequest.BodyRequestAsync.StartDate = queueItem.FileDate.ToString("yyyy-MM-dd");
            reportRequest.BodyRequestAsync.EndDate = queueItem.FileDate.ToString("yyyy-MM-dd");
            reportRequest.BodyRequestAsync.CustomFileName = $"{queueItem.FileGUID}_{report.APIReportName}";

            var reportResponse = await _rateLimiter.ExecuteAsync(async (_) => await reportRequest.FetchDataAsync<AsyncReportResponse>());

            reportItem.ReportToken = reportResponse.RunId;
            reportItem.TimeSubmitted = DateTime.UtcNow;
            return reportItem;
        }

        public async Task<List<ApiReportItem>> DownloadAllAvailableColumnsAsync(IEnumerable<SkaiProfile> skaiProfiles, Queue queueItem, APIReport<ReportSettings> report
            , Action<string, ApiReportItem, Stream> saveFileAction, Func<SkaiProfile, ColumnResponse, FileCollectionItem> transformDataAction)
        {
            List<ApiReportItem> apiReportItems = new();

            await MakeParallelCallsAsync(skaiProfiles, queueItem, report, "DownloadAllAvailableColumns"
            , (queueItem, report, profile) =>
            {
                var reportRequest = new ApiRequest(_endpointUri, _skaiOAuth, report.ReportSettings.Path, _httpClientProvider)
                {
                    ServerID = queueItem.EntityID,
                    MethodType = System.Net.Http.HttpMethod.Get
                };

                return GetAvailableColumnsAsync(queueItem, report, profile, reportRequest, saveFileAction, transformDataAction);
            }
            , (concurrentBag) => apiReportItems.AddRange(concurrentBag));

            return apiReportItems;
        }

        public async Task<List<ApiReportItem>> DownloadAllProfileMetricsAsync(IEnumerable<SkaiProfile> skaiProfiles, Queue queueItem, APIReport<ReportSettings> report
            , Action<string, int, ApiReportItem, Stream> saveFileAction)
        {
            List<ApiReportItem> apiReportItems = new();

            CustomMetricField[] standardColumns = report.ReportFields?.Select(x => x.APIReportFieldName.StartsWith(GROUP_NAME_PREFIX)
                ? new Greenhouse.Data.DataSource.Skai.CustomMetrics.CustomMetricField
                {
                    Group = ParseApiReportFieldName(x.APIReportFieldName, GROUP_NAME_PREFIX)[0],
                    Name = ParseApiReportFieldName(x.APIReportFieldName, GROUP_NAME_PREFIX)[1]
                }
                : new Greenhouse.Data.DataSource.Skai.CustomMetrics.CustomMetricField { Name = x.APIReportFieldName }
            ).ToArray();

            await MakeParallelCallsAsync(skaiProfiles, queueItem, report, "DownloadAllProfileMetrics"
            , (queueItem, report, profile) =>
            {
                var reportRequest = new ApiRequest(_endpointUri, _skaiOAuth, report.ReportSettings.Path, _httpClientProvider)
                {
                    ServerID = queueItem.EntityID,
                    MethodType = System.Net.Http.HttpMethod.Post
                };

                reportRequest.SetParameters();

                reportRequest.BodyRequest = new SyncReportRequest
                {
                    Entity = report.ReportSettings.Entity.ToString(),
                    DateRange = new Date_Range { StartDate = queueItem.FileDate.ToString("yyyy-MM-dd"), EndDate = queueItem.FileDate.ToString("yyyy-MM-dd") },
                    Limit = report.ReportSettings.PageLimit,
                };

                return GetCustomMetricsAsync(queueItem, report, profile.ProfileID, reportRequest, standardColumns, saveFileAction);
            }, (concurrentBag) => apiReportItems.AddRange(concurrentBag));

            return apiReportItems;
        }

        private async Task<ApiReportItem> GetAvailableColumnsAsync<TResponse>(Queue queueItem, APIReport<ReportSettings> report, SkaiProfile profile
            , ApiRequest apiRequest, Action<string, ApiReportItem, Stream> saveFileAction, Func<SkaiProfile, TResponse, FileCollectionItem> transformDataAction)
        {
            var reportItem = new ApiReportItem()
            {
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportName = report.APIReportName.ToLower(),
                ServerID = queueItem.EntityID,
                FileExtension = "json",
                ProfileID = profile.ProfileID,
                ApiReportType = report.ReportSettings.ReportType,
                ApiReportEntity = report.ReportSettings.Entity
            };

            _logMessage(LogLevel.Debug, $"{queueItem.FileGUID}-Start GetAvailableColumns: queueID: {queueItem.ID}->{reportItem.ReportName}->{apiRequest.UriPath}" +
                $"->ServerID:{queueItem.EntityID}->ProfileID:{profile.ProfileID}.");

            apiRequest.ProfileID = profile.ProfileID;

            apiRequest.SetParameters();

            TResponse apiResponse = await _rateLimiter.ExecuteAsync(async token =>
            {
                await using Stream responseStream = await apiRequest.FetchDataAsync(token);

                using StreamReader reader = new(responseStream);
                string content = await reader.ReadToEndAsync(token);

                responseStream.Seek(0, SeekOrigin.Begin);
                saveFileAction(profile.ProfileID, reportItem, responseStream);

                if (typeof(TResponse) == typeof(string))
                {
                    return (TResponse)(object)content;
                }

                return JsonConvert.DeserializeObject<TResponse>(content);
            });

            reportItem.FileItem = transformDataAction(profile, apiResponse);

            reportItem.IsReady = true;
            reportItem.IsDownloaded = true;

            return reportItem;
        }

        private async Task<ApiReportItem> GetCustomMetricsAsync(Queue queueItem, APIReport<ReportSettings> report, string profileID
            , ApiRequest apiRequest, CustomMetricField[] standardColumns, Action<string, int, ApiReportItem, Stream> saveFileAction)
        {
            var reportItem = new ApiReportItem()
            {
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportName = report.APIReportName.ToLower(),
                ServerID = queueItem.EntityID,
                ReportToken = "0",
                FileExtension = "json",
                ProfileID = profileID,
                ApiReportType = report.ReportSettings.ReportType,
                ApiReportEntity = report.ReportSettings.Entity
            };

            apiRequest.BodyRequest.ProfileId = Convert.ToInt32(profileID);

            var customFields = _savedColumns.Where(x => x.ProfileID == Convert.ToInt32(profileID)
                                && x.ServerID == Convert.ToInt32(queueItem.EntityID)
                                && Utilities.UtilsText.ConvertToEnum<ReportEntity>(x.Entity) == report.ReportSettings.Entity
                                && !standardColumns.Select(s => s.Name.ToLower()).Contains(x.ColumnName.ToLower()))
                                    .Select(c => new CustomMetricField { Group = c.GroupName, Name = c.ColumnName });

            var requestFields = standardColumns.Union(customFields).ToList();

            List<List<CustomMetricField>> customColumnLists = _guardrails.GetCustomColumnsLists(requestFields);

            if (customColumnLists.Count == 0)
            {
                reportItem.IsReady = true;
                reportItem.IsDownloaded = true;
                return reportItem;
            }

            int counter = 0;

            foreach (var customColumnList in customColumnLists)
            {
                apiRequest.BodyRequest.Fields = customColumnList.ToArray();

                string nextPageLink = "0";

                while (!string.IsNullOrEmpty(nextPageLink))
                {
                    _logMessage(LogLevel.Debug, $"{queueItem.FileGUID}-Start GetCustomMetrics: queueID: {queueItem.ID}->{reportItem.ReportName}->{apiRequest.UriPath}" +
                        $"->ServerID:{queueItem.EntityID}->ProfileID:{profileID}->Page:{nextPageLink}.");

                    apiRequest.BodyRequest.Page = Convert.ToInt32(nextPageLink);

                    SyncReportResponse newResponse = await _rateLimiter.ExecuteAsync(async token =>
                    {
                        await using Stream responseStream = await apiRequest.FetchDataAsync(token);

                        using StreamReader reader = new(responseStream);
                        string content = await reader.ReadToEndAsync(token);

                        responseStream.Seek(0, SeekOrigin.Begin);
                        saveFileAction(profileID, counter, reportItem, responseStream);

                        return JsonConvert.DeserializeObject<SyncReportResponse>(content);
                    });

                    counter++;
                    nextPageLink = newResponse.Paging.NextPage;
                }
            }

            reportItem.IsReady = true;
            reportItem.IsDownloaded = true;

            return reportItem;
        }

        private async Task MakeParallelCallsAsync<T, U>(IEnumerable<T> source, Queue queueItem, APIReport<ReportSettings> report
            , string methodCaller, Func<Queue, APIReport<ReportSettings>, T, Task<U>> makeRequest, Action<ConcurrentBag<U>> saveOutput)
        {
            ConcurrentBag<U> outputStaging = new();
            ConcurrentBag<Exception> exceptions = new();

            await Parallel.ForEachAsync(source, _apiParallelOptions, async (sourceItem, _) =>
            {
                try
                {
                    var output = await makeRequest(queueItem, report, sourceItem);
                    outputStaging.Add(output);
                }
                catch (Exception exc)
                {
                    _logException(LogLevel.Error, $"{queueItem.FileGUID}-Stopping Parallel.ForEach loop|Method:{methodCaller}|Exception:{exc.GetType().FullName}|Message:{exc.Message}|InnerExceptionMessage:{exc.InnerException?.Message}|STACK {exc.StackTrace}", exc);
                    exceptions.Add(exc);
                }
            });

            // save all active entities
            // then throw errors
            saveOutput(outputStaging);

            if (!exceptions.IsEmpty)
            {
                throw new ParallelProcessingException("Exception caught in Parallel.ForEach - {methodCaller}", exceptions.First());
            }
        }

        private static string[] ParseApiReportFieldName(string apiReportFieldName, string groupNamePrefix)
        {
            return apiReportFieldName.Replace(groupNamePrefix, "").Split('.');
        }
    }
}

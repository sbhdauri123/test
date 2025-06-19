using Greenhouse.Data.DataSource.Facebook;
using Greenhouse.Data.DataSource.Facebook.GraphApi.Core;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.DAL.DataSource.Facebook
{
    public class GraphApiInsights
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private readonly GraphApiRequest api;
        private readonly GraphApiBatchRequest batchApi;
        private readonly IHttpClientProvider _httpClientProvider;
        private string JobGuid { get; set; }

        public GraphApiInsights(IHttpClientProvider httpClientProvider, string endpointUri, string jobLogGuid, string apiKey, string urlExtension)
        {
            this.JobGuid = jobLogGuid;
            this.api = new GraphApiRequest(httpClientProvider, endpointUri, jobLogGuid, apiKey, urlExtension)
            {
                EntityName = "insights"
            };
            this.batchApi = new GraphApiBatchRequest(httpClientProvider, endpointUri, jobLogGuid, apiKey, urlExtension);
            _httpClientProvider = httpClientProvider;
        }

        /// <summary>
        /// Filter list of object IDs to ones with delivery data only
        /// </summary>
        /// <param name="listSummaryReport"></param>
        /// <param name="objectIdList"></param>
        /// <param name="usePreset"></param>
        /// <param name="fileDate"></param>
        /// <returns></returns>
        public List<ApiReportResponse> FilterObjectIdList(MappedReportsResponse<FacebookReportSettings> listSummaryReport, List<string> objectIdList, bool usePreset, DateTime fileDate)
        {
            var batchResponseList = new List<ApiReportResponse>();

            // create individual api requests
            var apiRequestList = new List<GraphApiRequest>();
            foreach (var id in objectIdList)
            {
                var apiRequest = new GraphApiRequest(_httpClientProvider, this.api.URLExtension)
                {
                    EntityName = "insights",
                    EntityId = id,
                    UseDateParameter = true,
                    UseDatePreset = usePreset
                };

                if (usePreset)
                {
                    apiRequest.UseDatePreset = true;
                }
                else
                {
                    apiRequest.StartTime = fileDate;
                    apiRequest.EndTime = fileDate;
                }

                apiRequest.SetParameters(listSummaryReport);
                apiRequestList.Add(apiRequest);
            }

            // batch request limited to 50 operations
            // make the 50 a lookup instead
            var requestBatches = Utilities.UtilsText.GetSublistFromList(apiRequestList, 50);

            foreach (var requestBatch in requestBatches)
            {
                var batchList = requestBatch.ToList();
                if (this.batchApi.BatchOperations.Count != 0)
                    this.batchApi.BatchOperations.Clear();

                var batchOperations = batchList
                    .Select(x =>
                     new BatchOperation
                     {
                         method = System.Net.Http.HttpMethod.Get.ToString(),
                         relative_url = x.UriPath
                     }
                );

                this.batchApi.BatchOperations.AddRange(batchOperations);

                var batchResponses = this.batchApi.FetchDataAsync<List<BatchResponse>>().GetAwaiter().GetResult();

                bool logUtilizationWarning = true;

                for (var k = 0; k < batchResponses.Count; k++)
                {
                    var responseItem = new ApiReportResponse
                    {
                        BatchItemResponse = batchResponses[k],
                        EntityID = batchList[k].EntityId
                    };

                    var responseTuple = Tuple.Create(batchList[k].EntityId, responseItem);

                    // check rate limit headers to see if we are close to having all calls throttled
                    if (responseItem.AppUtilizationPercentage > 95 || responseItem.AccountUtilizationPercentage > 95)
                    {
                        throw new FacebookApiThrottleException($"{this.JobGuid}-Utilization is above 95 pct|appPct={responseItem.AppUtilizationPercentage}|accountPct={responseItem.AccountUtilizationPercentage}|headers={String.Join(",", responseItem.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}" +
                            $"|Report={responseItem.ReportItem.ReportName}|status:{responseItem.ReportItem.EntityStatus}|url:{responseItem.ReportItem.RelativeUrl}|retryAfterInMinutes={responseItem.RetryAfterInMinutes}");
                    }
                    else if (logUtilizationWarning && (responseItem.AppUtilizationPercentage > 90 || responseItem.AccountUtilizationPercentage > 90))
                    {
                        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, $"{this.JobGuid}-Api rate limit is nearing threshold of 95 pct utilization|appPct={responseItem.AppUtilizationPercentage}|accountPct={responseItem.AccountUtilizationPercentage}|headers={String.Join(",", responseItem.GetBatchHeader().Select(header => $"{header.Key}:{header.Value}"))}" +
                            $"|Report={responseItem.ReportItem.ReportName}|status:{responseItem.ReportItem.EntityStatus}|url:{responseItem.ReportItem.RelativeUrl}|retryAfterInMinutes={responseItem.RetryAfterInMinutes}"));
                        logUtilizationWarning = false;
                    }

                    batchResponseList.Add(responseItem);
                }

                // if any requests error, then let's come out of this loop (and method) and allow caller to decide if we should continue or not
                var errorResponses = batchResponseList.Where(x => x.BatchItemResponse.Code != 200);

                if (errorResponses.Any())
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, $"Batch response coming back unsuccessful Total Failed:{errorResponses.Count()}-Ex:{errorResponses.First().BatchItemResponse.Code}-{errorResponses.First().BatchItemResponse.Body} " +
                        $"AccountID: {this.api.AccountID} EntityID:{this.api.EntityId};FileDate={fileDate}"));
                    break;
                }

                // delay between batch requests
                Task.Delay(300).Wait();
            }

            return batchResponseList;
        }
    }
}
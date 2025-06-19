using Greenhouse.Common;
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
    public class GraphApiRequest : GraphApiBaseRequest
    {
        public bool UseDateParameter { get; set; }
        public bool UseDatePreset { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        private string DateFormat { get; } = "yyyy-MM-dd";
        public string EntityName { get; set; } = "account";
        public string EntityId { get; set; }
        public string EntityLevel { get; set; }
        private string _parameters;
        public string Parameters
        {
            get
            {
                return _parameters;
            }
        }
        public override string HttpBody
        {
            get
            {
                return string.Empty;
            }
        }
        public bool IsStatusCheck { get; set; }
        public string ReportID { get; set; }
        public string PagingCursor { get; set; }

        public void SetParameters(MappedReportsResponse<FacebookReportSettings> report, bool isDeliveryDownload = false, string status = null, string limitOverride = null)
        {
            var parameters = new List<string>();

            // insights report downloads only use limit and page cursor, so we can skip these
            if (!isDeliveryDownload)
            {
                if (UseDateParameter)
                {
                    if (UseDatePreset && !string.IsNullOrEmpty(report.ReportSettings.DatePreset))
                    {
                        parameters.Add($"date_preset={report.ReportSettings.DatePreset}");
                    }
                    else
                    {
                        var startDate = StartTime.ToString(DateFormat);
                        var endDate = EndTime.ToString(DateFormat);
                        if (!string.IsNullOrEmpty(startDate) && !string.IsNullOrEmpty(endDate))
                        {
                            string dateRange = $"{{'since':'{StartTime.ToString(DateFormat)}','until':'{EndTime.ToString(DateFormat)}'}}";
                            parameters.Add($"time_range={dateRange}");
                        }
                    }
                }

                var fields = report.ReportFields.Select(s => s.APIReportFieldName).Aggregate((current, next) => current + "," + next);

                if (!string.IsNullOrEmpty(fields))
                {
                    parameters.Add($"fields={fields}");
                }

                if (!string.IsNullOrEmpty(status))
                {
                    parameters.Add($"status=[{status}]");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.TimeIncrement))
                {
                    parameters.Add($"time_increment={report.ReportSettings.TimeIncrement}");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.Level))
                {
                    parameters.Add($"level={report.ReportSettings.Level}");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.Breakdowns))
                {
                    parameters.Add($"breakdowns={report.ReportSettings.Breakdowns}");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.ActionBreakdowns))
                {
                    parameters.Add($"action_breakdowns={report.ReportSettings.ActionBreakdowns}");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.AttributionWindows))
                {
                    parameters.Add($"action_attribution_windows={report.ReportSettings.AttributionWindows}");
                }

                if (!string.IsNullOrEmpty(report.ReportSettings.Filtering))
                {
                    parameters.Add($"filtering=[{{field:'{report.ReportSettings.Filtering}',operator:'GREATER_THAN',value:0}}]");
                }
            }

            if (!string.IsNullOrEmpty(report.ReportSettings.Limit) && report.ReportSettings.ReportType != "account")
            {
                parameters.Add(string.IsNullOrEmpty(limitOverride) ? $"limit={report.ReportSettings.Limit}" : $"limit={limitOverride}");
            }

            if (!string.IsNullOrEmpty(this.PagingCursor))
            {
                parameters.Add($"after={this.PagingCursor}");
            }

            _parameters = string.Join("&", parameters);
        }

        public GraphApiRequest(IHttpClientProvider httpClientProvider, string endpointUri, string jobLogGuid, string apiKey, string urlExtension) : base(httpClientProvider)
        {
            this.EndpointUri = endpointUri;
            this.JobLogGuid = jobLogGuid;
            this.ApiKey = apiKey;
            this.URLExtension = urlExtension;
        }

        public GraphApiRequest(IHttpClientProvider httpClientProvider, string endpointUri, string jobLogGuid, string apiKey, string urlExtension, string accountId, string entityName, string entityId = null) : base(httpClientProvider)
        {
            this.EndpointUri = endpointUri;
            this.JobLogGuid = jobLogGuid;
            this.ApiKey = apiKey;
            this.URLExtension = urlExtension;
            this.AccountID = accountId;
            this.EntityName = entityName;
            this.EntityId = entityId;
        }

        // constructor for use with creating batch operations
        public GraphApiRequest(IHttpClientProvider httpClientProvider, string urlExtension) : base(httpClientProvider)
        {
            this.URLExtension = urlExtension;
        }

        //API Method
        //act_<AD_ACCOUNT_ID>/insights
        //<CAMPAIGN_ID>/insights
        //<ADSET_ID>/insights
        //<AD_ID>/insights
        public override string UriPath
        {
            get
            {
                var path = $"{URLExtension}";

                if (this.IsStatusCheck)
                {
                    return $"{path}/{ReportID}";
                }

                var entityName = EntityName?.ToLower();

                if (!string.IsNullOrEmpty(EntityId))
                {
                    if (!string.IsNullOrEmpty(entityName))
                    {
                        path = $"{path}/{EntityId}/{entityName}";
                    }
                    else
                    {
                        path = $"{path}/{EntityId}";
                    }
                }
                else if (!string.IsNullOrEmpty(AccountID))
                {
                    path = entityName == "account" ? $"{path}/act_{AccountID}" : $"{path}/act_{AccountID}/{entityName}";
                }

                return string.IsNullOrEmpty(Parameters) ? $"{path}" : $"{path}?{Parameters.TrimStart(Constants.AMPERSAND_ARRAY)}";
            }
        }

        public List<T> GetPagedData<T>(MappedReportsResponse<FacebookReportSettings> summaryReport, bool isDeliveryDownload)
        {
            var dataList = new List<T>();
            this.PagingCursor = string.Empty;
            int counter = 0;
            bool logUtilizationWarning = true;
            do
            {
                this.SetParameters(summaryReport, isDeliveryDownload);
                var reportData = FetchDataAsync<GraphData<T>>().GetAwaiter().GetResult();

                // check rate limit headers to see if we are close to having all calls throttled
                if (reportData.AppUtilizationPercentage > 95 || reportData.AccountUtilizationPercentage > 95)
                {
                    throw new FacebookApiThrottleException($"{this.JobLogGuid}-Utilization is above 95 pct|appPct={reportData.AppUtilizationPercentage}|accountPct={reportData.AccountUtilizationPercentage}|headers={String.Join(",", reportData.Header.Select(header => $"{header.Key}:{header.Value}"))}" +
                        $"|Report={summaryReport.APIReportName}|url:{this.UriPath}|retryAfterInMinutes={reportData.RetryAfterInMinutes}");
                }
                else if (logUtilizationWarning && (reportData.AppUtilizationPercentage > 90 || reportData.AccountUtilizationPercentage > 90))
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, $"{this.JobLogGuid}-Api rate limit is nearing threshold of 95 pct utilization|appPct={reportData.AppUtilizationPercentage}|accountPct={reportData.AccountUtilizationPercentage}|headers={String.Join(",", reportData.Header.Select(header => $"{header.Key}:{header.Value}"))}" +
                        $"|Report={summaryReport.APIReportName}|url:{this.UriPath}|retryAfterInMinutes={reportData.RetryAfterInMinutes}"));
                    logUtilizationWarning = false;
                }

                dataList.AddRange(reportData.data);
                if (reportData?.paging?.next != null && reportData?.paging?.cursors?.after != null)
                {
                    this.PagingCursor = reportData?.paging?.cursors?.after;
                }
                else
                {
                    this.PagingCursor = string.Empty;
                }

                counter++;
                Task.Delay(300).Wait();
            } while (!string.IsNullOrEmpty(this.PagingCursor));

            return dataList;
        }
    }
}

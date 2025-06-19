using Greenhouse.Data.DataSource.Facebook.GraphApi.Core;
using Greenhouse.Data.DataSource.Facebook.GraphApi.HeaderInfo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Greenhouse.Data.DataSource.Facebook
{
    public class ApiReportResponse
    {
        public const string BUC_HEADER_NAME = "x-business-use-case-usage";
        public const string INSIGHTS_THROTTLE_HEADER_NAME = "x-fb-ads-insights-throttle";
        public const string APP_USAGE_HEADER_NAME = "x-app-usage";
        public const string ACCOUNT_USAGE_HEADER_NAME = "x-ad-account-usage";

        public Dictionary<string, string> Header { get; set; }
        public HttpStatusCode ResponseCode { get; set; }
        public BatchResponse BatchItemResponse { get; set; }
        public FacebookReportItem ReportItem { get; set; }
        public ApiError ApiError { get; set; }
        public string EntityID { get; set; }
        public Dictionary<string, string> GetBatchHeader()
        {
            var headerDictionary = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

            if (this.BatchItemResponse?.headers != null)
            {
                headerDictionary = BatchItemResponse.headers.ToDictionary(x => x.Name, x => x.Value, StringComparer.InvariantCultureIgnoreCase);
            }

            return headerDictionary;
        }
        public GraphData<T> GetResponseData<T>()
        {
            if (string.IsNullOrEmpty(BatchItemResponse?.Body))
            {
                return null;
            }

            return Newtonsoft.Json.JsonConvert.DeserializeObject<GraphData<T>>(BatchItemResponse?.Body);
        }

        public string ErrorMessage
        {
            get
            {
                if (this.ApiError?.Error?.Message != null)
                {
                    return this.ApiError.Error.Message;
                }
                else
                {
                    return "";
                }
            }
        }

        public BusinessUseCaseUsageHeader BusinessUseCaseUsage
        {
            get
            {
                var headerDictionary = Header ?? GetBatchHeader();
                if (!headerDictionary.TryGetValue(BUC_HEADER_NAME, out string value))
                    return null;

                return Newtonsoft.Json.JsonConvert.DeserializeObject<BusinessUseCaseUsageHeader>(value);
            }
        }
        public InsightsThrottleHeader InsightsThrottleInfo
        {
            get
            {
                var headerDictionary = Header ?? GetBatchHeader();
                if (!headerDictionary.TryGetValue(INSIGHTS_THROTTLE_HEADER_NAME, out string value))
                    return null;

                return Newtonsoft.Json.JsonConvert.DeserializeObject<InsightsThrottleHeader>(value);
            }
        }
        public AppUsageHeader AppUsage
        {
            get
            {
                var headerDictionary = Header ?? GetBatchHeader();
                if (!headerDictionary.TryGetValue(APP_USAGE_HEADER_NAME, out string value))
                    return null;

                return Newtonsoft.Json.JsonConvert.DeserializeObject<AppUsageHeader>(value);
            }
        }
        public AccountUsageHeader AccountUsage
        {
            get
            {
                var headerDictionary = Header ?? GetBatchHeader();
                if (!headerDictionary.TryGetValue(ACCOUNT_USAGE_HEADER_NAME, out string value))
                    return null;

                return Newtonsoft.Json.JsonConvert.DeserializeObject<AccountUsageHeader>(value);
            }
        }

        public int AppUtilizationPercentage
        {
            get
            {
                var totalPercentage = 0;

                if (InsightsThrottleInfo != null)
                {
                    bool result = int.TryParse(InsightsThrottleInfo.AppIdUtilPct, out totalPercentage);
                    if (result)
                        return totalPercentage;
                }

                return totalPercentage;
            }
        }
        public int AccountUtilizationPercentage
        {
            get
            {
                var totalPercentage = 0;

                if (InsightsThrottleInfo != null)
                {
                    bool result = int.TryParse(InsightsThrottleInfo.AccIdUtilPct, out totalPercentage);
                    if (result)
                        return totalPercentage;
                }

                return totalPercentage;
            }
        }

        public int RetryAfterInMinutes
        {
            get
            {
                var totalMinutes = 0;

                if (BusinessUseCaseUsage != null)
                {
                    totalMinutes = BusinessUseCaseUsage.BucInfo.Max(x => x.EstimatedTimeToRegainAccess);
                }

                return totalMinutes;
            }
        }
    }
}

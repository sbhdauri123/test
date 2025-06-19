using Greenhouse.Common;
using Greenhouse.Data.DataSource.DBM.API.Resource;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Greenhouse.Data.DataSource.DBM.API
{
    [Serializable]
    public class ApiReportRequest
    {
        public IEnumerable<Model.Aggregate.APIReportField> Dimensions { get; set; }
        public IEnumerable<Model.Aggregate.APIReportField> Metrics { get; set; }
        public string ProfileID { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public IEnumerable<Filter> AdditionalFilters { get; set; }
        public DataRange ReportDataRange { get; set; } = DataRange.CUSTOM_DATES;
        public DateTime StartDate { get; set; } = DateTime.Now.Date;
        public DateTime EndDate { get; set; } = DateTime.Now.Date;
        public bool IsStatusCheck { get; set; }
        public long ReportID { get; set; }
        public string ReportName { get; set; }
        public long QueryID { get; set; }
        private string _parameters;
        public string Parameters
        {
            get
            {
                return _parameters;
            }
        }
        public void SetParameters(APIReport<ReportSettings> apiReport, string nextPageUrl = null)
        {
            var parameterList = new List<string>();
            if (!string.IsNullOrEmpty(apiReport.ReportSettings.PageSize))
            {
                parameterList.Add($"pageSize={apiReport.ReportSettings.PageSize}");
            }

            if (!string.IsNullOrEmpty(apiReport.ReportSettings.Fields))
            {
                parameterList.Add($"fields={WebUtility.UrlEncode(apiReport.ReportSettings.Fields)}");
            }

            if (!string.IsNullOrEmpty(nextPageUrl))
            {
                parameterList.Add($"pageToken={WebUtility.UrlEncode(nextPageUrl)}");
            }

            if (!string.IsNullOrEmpty(apiReport.ReportSettings.OrderBy))
            {
                parameterList.Add($"orderBy={WebUtility.UrlEncode(apiReport.ReportSettings.OrderBy)}");
            }

            _parameters = string.Join("&", parameterList);
        }

        public string GetReportRequestBody(APIReport<ReportSettings> apiReport)
        {
            var reportType = UtilsText.ConvertToEnum<ReportType>(apiReport.ReportSettings.ReportType);

            Metrics = null;
            if (apiReport.ReportSettings.UseMetrics && apiReport.ReportFields.Any())
            {
                Metrics = apiReport.ReportFields.Where(x => !x.IsDimensionField);
            }

            Dimensions = null;
            if (apiReport.ReportSettings.UseDimensions && apiReport.ReportFields.Any())
            {
                Dimensions = apiReport.ReportFields.Where(x => x.IsDimensionField);
            }

            var customDataRange = new QueryDataRange { Range = ReportDataRange };

            if (ReportDataRange == DataRange.CUSTOM_DATES)
            {
                customDataRange.QueryDataStartDate = new ResourceDate { Year = StartDate.Year, Month = StartDate.Month, Day = StartDate.Day };
                customDataRange.QueryDataEndDate = new ResourceDate { Year = EndDate.Year, Month = EndDate.Month, Day = EndDate.Day };
            }

            var metaData = new QueryMetadata()
            {
                DataRange = customDataRange,
                Format = ReportFormat.CSV,
                Title = $"Daily_{reportType}_Report"
            };

            var reportParams = new ResourceParameters
            {
                Filters = new List<Filter> { new Filter { Type = FilterType.FILTER_PARTNER, Value = ProfileID } },
                Metrics = Metrics?.Select(x => x.APIReportFieldName)?.ToList(),
                GroupBys = Dimensions?.Select(x => x.APIReportFieldName)?.ToList(),
                Type = reportType
            };

            if (AdditionalFilters != null && AdditionalFilters.Any())
                reportParams.Filters.AddRange(AdditionalFilters);

            var report = new RestQuery()
            {
                Metadata = metaData,
                Params = reportParams
            };

            return Newtonsoft.Json.JsonConvert.SerializeObject(report, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        public string UriPath
        {
            get
            {
                var path = "queries";

                if (IsStatusCheck)
                {
                    MethodType = System.Net.Http.HttpMethod.Get;
                    path = (ReportID > 0) ? $"{path}/{QueryID}/reports/{ReportID}" : $"{path}/{QueryID}/reports";
                }
                else if (QueryID > 0)
                {
                    path = $"{path}/{QueryID}:run";
                }

                return string.IsNullOrEmpty(Parameters) ? $"{path}" : $"{path}?{Parameters.TrimStart(Constants.AMPERSAND_ARRAY)}";
            }
        }
    }
}
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.DCM
{
    [Serializable]
    public class ApiCreateReportRequest
    {
        public IEnumerable<Model.Aggregate.APIReportField> Dimensions { get; set; }
        public IEnumerable<Model.Aggregate.APIReportField> Metrics { get; set; }
        public string ProfileID { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }

        private string DateFormat { get; } = "yyyy-MM-dd";

        public string GetReportRequestBody()
        {
            var criterias = new ReportCriteria()
            {
                dateRange = new ReportDateRange()
                {
                    endDate = EndDate.ToString(DateFormat),
                    startDate = StartDate.ToString(DateFormat)
                }
                ,
                metricNames = Metrics?.Select(x => x.APIReportFieldName)
                ,
                dimensions = Dimensions?.Select(x => new ReportDimension() { name = x.APIReportFieldName })
            };

            var report = new Report()
            {
                type = ReportTypes.STANDARD.ToString(),
                name = $"{ReportTypes.STANDARD.ToString()}_{ProfileID}_{StartDate.ToString("yyyyMMdd")}_{EndDate.ToString("yyyyMMdd")}",
            };

            report.criteria = criterias;

            return Newtonsoft.Json.JsonConvert.SerializeObject(report, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
        }

        public string UriPath { get { return $"userprofiles/{ProfileID}/reports"; } }
    }

    public class ReportCriteria
    {
        public IEnumerable<ReportDimension> dimensions { get; set; }
        public IEnumerable<string> metricNames { get; set; }
        public ReportDateRange dateRange { get; set; }
    }

    public class ReportDimension
    {
        public string name { get; set; }
    }

    public class ReportDateRange
    {
        public string startDate { get; set; }
        public string endDate { get; set; }
        public string kind { get; set; } = string.Empty;
    }
    public class Report
    {
        public string name { get; set; }
        public string type { get; set; }
        public ReportCriteria criteria { get; set; }
    }

    public class ApiReportRequest
    {
        public string ProfileID { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;
        public long ReportID { get; set; }
        public bool IsStatusCheck { get; set; }
        public string FileID { get; set; }
        public string UriPath
        {
            get
            {
                if (IsStatusCheck)
                {
                    MethodType = System.Net.Http.HttpMethod.Get;
                    return $"userprofiles/{ProfileID}/reports/{ReportID}/files/{FileID}";
                }
                else
                {
                    MethodType = System.Net.Http.HttpMethod.Post;
                    return $"userprofiles/{ProfileID}/reports/{ReportID}/run";
                }
            }
        }
    }

    public class ApiDimensionRequest
    {
        public string ProfileID { get; set; }
        public System.Net.Http.HttpMethod MethodType { get; set; } = System.Net.Http.HttpMethod.Post;

        public string UriPath { get { return $"userprofiles/{ProfileID}"; } }
    }
}
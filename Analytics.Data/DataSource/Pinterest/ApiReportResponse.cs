using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;

namespace Greenhouse.Data.DataSource.Pinterest
{
    [Serializable]
    public class MetricReportResponse
    {
        [JsonProperty("report_status")]
        public string ReportStatus { get; set; }
        [JsonProperty("code")]
        public string Code { get; set; }
        [JsonProperty("token")]
        public string Token { get; set; }
        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("endpoint_name")]
        public string EndpointName { get; set; }
        [JsonProperty("error")]
        public Error Error { get; set; }
        [JsonProperty("message_detail")]
        public string MessageDetail { get; set; }
        public Dictionary<string, string> Header { get; set; }
        public HttpStatusCode ResponseCode { get; set; }
        [JsonProperty("url")]
        public string URL { get; set; }
    }

    [Serializable]
    public class DimReportResponse : MetricReportResponse
    {
        [JsonProperty("items")]
        public dynamic Items { get; set; }

        [JsonProperty("bookmark")]
        public string NextPageTag { get; set; }
    }

    public class Error
    {
        [JsonProperty("message")]
        public string Message { get; set; }
    }
}
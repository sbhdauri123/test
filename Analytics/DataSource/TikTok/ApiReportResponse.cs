using Newtonsoft.Json;
using System;

namespace Greenhouse.Data.DataSource.TikTok
{
    [Serializable]
    public class ReportResponse
    {
        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("code")]
        public int Code { get; set; }
        [JsonProperty("data")]
        public ReportData ReportData { get; set; }
        [JsonProperty("request_id")]
        public string RequestId { get; set; }
    }
    public class ReportData
    {
        [JsonProperty("task_id")]
        public string TaskId { get; set; }
        [JsonProperty("status")]
        public string ReportStatus { get; set; }
        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("page_info")]
        public PageInfo PageInfo { get; set; }
    }

    public class PageInfo
    {
        [JsonProperty("total_number")]
        public int TotalNumber { get; set; }
        [JsonProperty("page")]
        public int Page { get; set; }
        [JsonProperty("page_size")]
        public int PageSize { get; set; }
        [JsonProperty("total_page")]
        public int TotalPage { get; set; }
    }
}
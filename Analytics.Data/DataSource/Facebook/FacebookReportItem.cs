using Greenhouse.Data.DataSource.Facebook.Core;
using Greenhouse.Data.Model.Core;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Facebook
{
    public class FacebookReportItem
    {
        public FacebookReportItem() { }
        public string ReportName { get; set; }
        public string AccountID { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string OriginalUrl { get; set; }
        public string RelativeUrl { get; set; }
        public string OriginalInsightsUrl { get; set; }
        public string RelativeInsightsUrl { get; set; }
        public FileCollectionItem FileCollectionItem { get; set; }
        public string ReportRunId { get; set; }
        public string EntityID { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }
        public bool IsDaily { get; set; }
        public int RetryAttempt { get; set; }
        public bool SkipEntity { get; set; }
        public bool RetryPageSize { get; set; }
        public string PageSize { get; set; }
        public string EntityStatus { get; set; }
        public bool DownloadFailed { get; set; }
        public bool StatusCheckFailed { get; set; }
        public string ReportLevel { get; set; }
        private ApiTimeTracker _timeTracker { get; set; }
        public ApiTimeTracker TimeTracker
        {
            get
            {
                if (_timeTracker == null)
                    _timeTracker = new ApiTimeTracker();
                return _timeTracker;
            }
        }

        public static string GetNextPageSize(List<string> listPageSizeRetries, string currentLimit)
        {
            var nextPageSize = "";
            if (int.TryParse(currentLimit, out int limit))
            {
                var listPageSizeInt = listPageSizeRetries.Select(str =>
                {
                    bool success = int.TryParse(str, out int value);
                    return new { value, success };
                }).Where(pair => pair.success).Select(pair => pair.value).OrderByDescending(x => x);

                var pageOptions = listPageSizeInt.Where(x => x < limit);
                if (pageOptions.Any())
                {
                    nextPageSize = pageOptions.First().ToString();
                }
            }

            return nextPageSize;
        }
    }
}

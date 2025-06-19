using System;
using System.Collections.Generic;
using System.Linq;

namespace Greenhouse.Data.DataSource.Innovid
{
    public class ApiBundle
    {
        /// <summary>
        /// Once the Import is done, only 1 queue for that day+group will be updated to Import Success
        /// It will contain all the reports for that day+group
        /// </summary>
        public long MainQueueId { get; set; }
        public Guid MainFileGuid { get; set; }
        public DateTime FileDate { get; set; }
        public bool IsDaily { get; set; }
        public List<string> ClientIds { get; set; }
        public List<long> QueueIds { get; set; }
        public List<ApiReportItem> Reports { get; set; }
        public int RowNumber { get; set; }

        public void SetReports(Model.Ordered.OrderedQueue queueItem,
            IEnumerable<Model.Aggregate.APIReport<Model.Innovid.InnovidReportSettings>> reports,
            int dailyLookback)
        {
            // Contains the list of reports submitted per day
            // to be used later on to check status and to download reports
            var reportList = new List<ApiReportItem>();
            string dateFormat = "yyyy-MM-dd";

            // diat-9623. Daily jobs will pull data with -3 day rolling window from queue.FileDate. Backfills will remain as-is.
            var addDays = this.IsDaily ? -dailyLookback : 0;

            var date = new
            {
                Start = this.FileDate.AddDays(addDays).ToString(dateFormat),
                End = this.FileDate.ToString(dateFormat)
            };

            foreach (var report in reports)
            {
                var body = new ReportRequestBody
                {
                    ReportType = report.ReportSettings.ReportType,
                    Clients = this.ClientIds,
                    DateFrom = date.Start,
                    DateTo = date.End,
                    Fields = report.ReportFields.Select(x => x.APIReportFieldName).ToList()
                };

                reportList.Add(new ApiReportItem()
                {
                    ReportRequestBody = body,
                    ReportType = report.ReportSettings.ReportType,
                    ReportName = report.ReportSettings.ReportName,
                    Date = this.FileDate,
                    FileGuid = this.MainFileGuid,
                    QueueID = this.MainQueueId,
                    PriorityNumber = this.RowNumber
                });
            }

            this.Reports = reportList;
        }
    }
}

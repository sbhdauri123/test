using System;

namespace Greenhouse.Data.DataSource.Euromonitor
{
    public class ExecutionPlan<T>
    {
        public ExecutionPlan(T request, string reportName, string reportExtension)
        {
            Request = request;
            ReportName = reportName;
            ReportExtension = reportExtension;
        }

        public T Request { get; private set; }
        public string DownloadLink { get; set; }
        public string JobId { get; set; }
        public string ReportName { get; private set; }
        public string ReportExtension { get; private set; }
        public string ProcessingStatus { get; set; }
        public DateTimeOffset ScheduleJob { get; set; }
        public bool NotCreated => string.IsNullOrEmpty(ProcessingStatus);
        public bool InProgress => ProcessingStatus == nameof(Euromonitor.ProcessingStatus.Queued) || ProcessingStatus == nameof(Euromonitor.ProcessingStatus.Processing) || ProcessingStatus == nameof(Euromonitor.ProcessingStatus.Processed);
    }
}

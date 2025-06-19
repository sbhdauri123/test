using System;

namespace Greenhouse.Data.DataSource.Skai
{
    [Serializable]
    public class ApiReportItem
    {
        public ApiReportItem() { }
        public string ReportName { get; set; }
        public long QueueID { get; set; }
        public Guid FileGuid { get; set; }
        public string ServerID { get; set; }
        public string ProfileID { get; set; }
        public FusionReportStatus Status { get; set; }
        public string ReportToken { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }
        public bool IsFailed { get; set; }
        public string FileName { get; set; }
        public Model.Core.FileCollectionItem FileItem { get; set; }
        public DateTime? FileDate { get; set; }
        public string FileExtension { get; set; }
        public DateTime? TimeSubmitted { get; set; }
        public string ApiEndpoint { get; set; }
        public ReportType ApiReportType { get; set; }
        public ReportEntity ApiReportEntity { get; set; }
    }

    public enum FusionReportStatus
    {
        #region undocumented status:
        PENDING,
        RUNNING,
        FAILED_DATA_NOT_AVAILABLE,
        UNKNOWN, // assigned when unable to parse status as enum
        #endregion
        #region per API docs:
        COMPLETED, // Your report is ready to download.
        COMPLETED_WITH_ERRORS, // The report run was completed but there were some errors during report generation. If this issue persists, please contact Skai support.
        PARTIALLY_COMPLETED, // Some data may be missing from the report. If this issue persists, please contact Skai support.
        FAILED, // Skai could not run this report. If this issue persists, please contact Skai support.
        ABORTED, // Skai had to abort this report run. If this issue persists, please contact Skai support.
        FAILED_DATA_NOT_READY // The data for the requested dates is not yet ready in Skai (this data is pulled into Skai from the publishers and from external vendors). Abort the flow and try again in an hour.
        #endregion

    }
}
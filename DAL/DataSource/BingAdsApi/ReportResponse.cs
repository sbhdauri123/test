namespace Greenhouse.DAL.BingAds.Reporting
{
    public partial class ReportResponse
    {
        public string ReportType { get; set; }
        public bool IsReady { get; set; }
        public bool IsDownloaded { get; set; }

        public string ReportName { get; set; }

        public string ReportURL { get; set; }
        public ReportRequestStatusType Status { get; set; } = ReportRequestStatusType.Pending;
        public long QueueID { get; set; }

        public SubmitGenerateReportResponse ApiResponse { get; set; }
    }
}

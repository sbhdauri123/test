using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using System;

namespace Greenhouse.Data.DataSource.Snapchat;

[Serializable]
public class ApiReportItem
{
    private ApiReportItem() { }

    public ApiReportItem(Queue queueItem, APIReport<ReportSettings> report, ApiReportRequest reportRequest, string Url, int counter)
    {
        QueueID = queueItem.ID;
        FileGuid = queueItem.FileGUID;
        ReportName = report.APIReportName;
        ProfileID = reportRequest.AccountID;
        OrganizationID = reportRequest.OrganizationID;
        ReportType = report.ReportSettings.ReportType;
        ReportURL = Url;

        // add attribution combo to name to enforce uniqueness
        string attributionName = "";
        if (!string.IsNullOrEmpty(report.ReportSettings.SwipeUpAttributionWindow))
        {
            attributionName += $"_{report.ReportSettings.SwipeUpAttributionWindow}";
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.ViewAttributionWindow))
        {
            attributionName += $"_{report.ReportSettings.ViewAttributionWindow}";
        }

        FileName = GenerateFileName(queueItem, ReportName, reportRequest.EntityId, attributionName, counter);
    }

    public static string GenerateFileName(Queue queueItem, string reportName, string entityId = "", string attributionName = "", int fileCounter = 0)
    {
        return $"{queueItem.FileGUID.ToString().ToLower()}_{entityId?.ToLower()}_{reportName.ToLower()}{attributionName.ToLower()}_{fileCounter}.json".Replace("__", "_");
    }

    public string ReportName { get; }
    public long QueueID { get; }
    public Guid FileGuid { get; }
    public string ProfileID { get; }
    public string OrganizationID { get; }
    public long ReportID { get; }
    public string ReportURL { get; }
    public string FileName { get; }
    public string FileExtension { get; }
    public string FilePath => FileUri.LocalPath;
    public Uri FileUri { get; }
    public string ReportType { get; }
}

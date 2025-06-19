using Greenhouse.Data.DataSource.Facebook;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Newtonsoft.Json;
using NLog;
using Polly;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Greenhouse.DAL.DataSource.Facebook.Orchestration;

public class ReportManager
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();

    public string JobGuid { get; set; }
    public List<Snapshot> Snapshots { get; private set; }
    public IFile SavedSnapshots { get; set; }
    public List<IdVault> SavedIdVaults { get; private set; }
    public IFile SavedDimension { get; set; }


    public ReportManager(string jobGUID, RemoteAccessClient rac, Uri destinationUri, string entityID, long integrationID)
    {
        this.JobGuid = jobGUID;

        //create Snapshot File
        var paths = new string[] { $"{entityID}", integrationID.ToString(), "snapshots.json" };
        var fileName = Utilities.RemoteUri.CombineUri(destinationUri, paths);
        this.SavedSnapshots = rac.WithFile(fileName);
        this.Snapshots = new List<Snapshot>();

        //create Downloaded Ad Dimension list File
        var dimnsionPaths = new string[] { $"{entityID}", integrationID.ToString(), "downloaded_dimension.json" };
        var dimensionFileName = Utilities.RemoteUri.CombineUri(destinationUri, dimnsionPaths);
        this.SavedDimension = rac.WithFile(dimensionFileName);
        this.SavedIdVaults = new List<IdVault>();
    }

    #region Snapshot Actions
    /// <summary>
    /// Load snapshots saved in s3
    /// </summary>
    public void LoadSnapshots(Policy downloadPolicy)
    {
        string content = string.Empty;

        downloadPolicy.Execute(() =>
        {
            if (this.SavedSnapshots.Exists)
            {
                using (var sr = new StreamReader(this.SavedSnapshots.Get()))
                {
                    content = sr.ReadToEnd();
                }
            }
        });

        var fileSnapshots = JsonConvert.DeserializeObject<List<Snapshot>>(content) ?? new List<Snapshot>();

        // per facebook docs: https://developers.facebook.com/docs/marketing-api/insights/best-practices
        // Do not store the report_run_id for long term use, it expires after 30 days
        var minus30Date = DateTime.UtcNow.AddDays(-30);
        var expiredSnapshots = fileSnapshots.Where(x => x.SnapshotDate.Date != default && x.SnapshotDate.Date < minus30Date.Date).Select(x => x.QueueID).Distinct();
        if (expiredSnapshots.Any())
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, $"{JobGuid}-Removing stale snapshot queue IDs: {string.Join(",", expiredSnapshots)}"));
            fileSnapshots.RemoveAll(x => expiredSnapshots.Contains(x.QueueID));
        }

        this.Snapshots = fileSnapshots;
    }

    public void ClearSnapshot(Queue queueItem)
    {
        // clear snapshot
        var currentSnapshot = this.Snapshots.Find(x => x.QueueID == queueItem.ID);
        this.Snapshots.Remove(currentSnapshot);

        // save info in s3
        if (this.SavedSnapshots.Exists)
            this.SavedSnapshots.Delete();

        // convert string to stream
        byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this.Snapshots));

        using (MemoryStream stream = new MemoryStream(byteArray))
        {
            this.SavedSnapshots.Put(stream);
        }
    }

    /// <summary>
    /// Update Snapshots and save to s3
    /// </summary>
    /// <param name="reportList"></param>
    public void TakeSnapshot(List<FacebookReportItem> reportList, Queue queueItem)
    {
        // update Snapshots
        var matchingSnapshot = this.Snapshots.Find(x => x.QueueID == queueItem.ID);

        if (matchingSnapshot != null)
        {
            foreach (var reportItem in reportList)
            {
                var reportSnaphot = matchingSnapshot.PendingReports.Find(x => x.ReportID == reportItem.ReportRunId && x.ReportName.Equals(reportItem.ReportName, StringComparison.InvariantCultureIgnoreCase));
                if (reportSnaphot != null)
                {
                    reportSnaphot.Resubmit = reportItem.DownloadFailed || reportItem.StatusCheckFailed;
                    reportSnaphot.Url = reportItem.OriginalInsightsUrl;
                }
                else
                {
                    matchingSnapshot.PendingReports.Add(new SavedReportDetails(reportItem));
                }
            }

            matchingSnapshot.SnapshotDate = DateTime.UtcNow;
        }
        else
        {
            // create new snapshot if none exists
            var newSnapshot = new Snapshot()
            {
                QueueID = queueItem.ID,
                PendingReports = reportList.ConvertAll(reportItem => new SavedReportDetails(reportItem)),
                SnapshotDate = DateTime.UtcNow
            };

            this.Snapshots.Add(newSnapshot);
        }

        // save info in s3
        if (this.SavedSnapshots.Exists)
            this.SavedSnapshots.Delete();

        // convert string to stream
        byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this.Snapshots));

        using (MemoryStream stream = new MemoryStream(byteArray))
        {
            this.SavedSnapshots.Put(stream);
        }
    }
    #endregion

    #region Report Actions
    public static List<FacebookReportItem> CreateReportItems(Queue queueItem, MappedReportsResponse<FacebookReportSettings> report, bool isDailyJob
        , bool isDimension, GraphApiRequest reportRequest, bool usePreset, List<string> entityIdList = null)
    {
        var reportItems = new List<FacebookReportItem>();
        var fileCollectionItem = new FileCollectionItem
        {
            SourceFileName = report.APIReportName,
            FilePath = $"{report.APIReportName}_{queueItem.FileGUID}_{queueItem.FileDate:yyyy-MM-dd}.json",
            FileSize = 0,
        };

        if (!isDimension)
        {
            reportRequest.UseDateParameter = true;

            if (usePreset)
            {
                reportRequest.UseDatePreset = true;
            }
            else
            {
                reportRequest.StartTime = queueItem.FileDate;
                reportRequest.EndTime = queueItem.FileDate;
            }
        }

        if (isDimension && !string.IsNullOrEmpty(report.ReportSettings.DailyStatus) && isDailyJob)
        {
            var effectiveStatus = report.ReportSettings.DailyStatus;
            var statusList = effectiveStatus.Split(',').ToList();

            foreach (var status in statusList)
            {
                reportRequest.SetParameters(report, false, status);

                var reportItem = new FacebookReportItem()
                {
                    ReportName = fileCollectionItem.SourceFileName,
                    AccountID = reportRequest.AccountID,
                    QueueID = queueItem.ID,
                    FileGuid = queueItem.FileGUID,
                    OriginalUrl = reportRequest.UriPath,
                    RelativeUrl = reportRequest.UriPath,
                    FileCollectionItem = fileCollectionItem,
                    IsDaily = isDailyJob,
                    PageSize = report.ReportSettings.Limit,
                    EntityStatus = status,
                    ReportLevel = report.ReportSettings.Level
                };
                reportItems.Add(reportItem);
            }
        }
        else if (entityIdList.Count != 0)
        {
            for (int i = 0; i < entityIdList.Count; i++)
            {
                if (isDimension && !string.Equals(report.ReportSettings.Entity, "adcreatives", StringComparison.OrdinalIgnoreCase))
                {
                    reportRequest.EntityName = string.Empty;
                }
                reportRequest.EntityLevel = report.ReportSettings.Level;
                reportRequest.EntityId = entityIdList[i];
                reportRequest.SetParameters(report);

                var reportItem = new FacebookReportItem()
                {
                    ReportName = fileCollectionItem.SourceFileName,
                    AccountID = reportRequest.AccountID,
                    QueueID = queueItem.ID,
                    FileGuid = queueItem.FileGUID,
                    OriginalUrl = reportRequest.UriPath,
                    RelativeUrl = reportRequest.UriPath,
                    FileCollectionItem = fileCollectionItem,
                    IsDaily = isDailyJob,
                    PageSize = report.ReportSettings.Limit,
                    EntityID = reportRequest.EntityId,
                    ReportLevel = report.ReportSettings.Level
                };
                reportItems.Add(reportItem);
            }
        }
        else
        {
            reportRequest.SetParameters(report);

            var reportItem = new FacebookReportItem()
            {
                ReportName = fileCollectionItem.SourceFileName,
                AccountID = reportRequest.AccountID,
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                OriginalUrl = reportRequest.UriPath,
                RelativeUrl = reportRequest.UriPath,
                FileCollectionItem = fileCollectionItem,
                IsDaily = isDailyJob,
                PageSize = report.ReportSettings.Limit,
                ReportLevel = report.ReportSettings.Level
            };
            reportItems.Add(reportItem);
        }

        return reportItems;
    }

    public List<FacebookReportItem> GetInsightsReportItems(Queue queueItem, MappedReportsResponse<FacebookReportSettings> factReport, List<DataAdDimension> adDimensionList, GraphApiRequest reportRequest, bool usePreset)
    {
        var insightsReportItems = new List<FacebookReportItem>();

        var entityIdList = GetIdsForInsights(queueItem, factReport, adDimensionList, out List<SavedReportDetails> submittedReports);
        reportRequest.AccountID = queueItem.EntityID;
        reportRequest.EntityName = factReport.ReportSettings.ReportType;

        var idReportItems = new List<FacebookReportItem>();

        if (entityIdList.Count != 0)
        {
            var pendingIdReportItems = CreateReportItems(queueItem, factReport, !queueItem.IsBackfill, false, reportRequest, usePreset, entityIdList);

            idReportItems.AddRange(pendingIdReportItems);
        }

        if (submittedReports.Count != 0)
        {
            foreach (var report in submittedReports)
            {
                if (report.ReportID == "0")
                    continue;
                var submittedIdReportItems = CreateReportItems(queueItem, factReport, !queueItem.IsBackfill, false, reportRequest, usePreset, new List<string> { report.EntityID });
                var reportItem = submittedIdReportItems.FirstOrDefault();
                if (reportItem != null)
                {
                    reportItem.ReportRunId = report.ReportID;
                    reportItem.OriginalInsightsUrl = report.Url;
                    reportItem.RelativeInsightsUrl = report.Url;
                    idReportItems.Add(reportItem);
                }
            }
        }

        insightsReportItems.AddRange(idReportItems);

        if (insightsReportItems.Count == 0)
        {
            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    $"No {factReport.APIReportName} IDs were found to have delivery data! Creating empty reports. AccountID: {queueItem.EntityID}; " +
                    $"File Date: {queueItem.FileDate}; total ad IDs: {entityIdList.Count}"));
            //nothing to report on, so create report items and assign zero report ID with ready and downloaded flags
            //these will be empty reports later when we upload all completed reports to S3
            var reportItems = CreateReportItems(queueItem, factReport, !queueItem.IsBackfill, false, reportRequest, usePreset, entityIdList);
            foreach (var reportItem in reportItems)
            {
                reportItem.ReportRunId = "0";
                reportItem.IsReady = true;
                reportItem.IsDownloaded = true;
            }

            insightsReportItems.AddRange(reportItems);
        }

        return insightsReportItems;
    }

    public List<string> GetIdsForInsights(Queue queueItem, MappedReportsResponse<FacebookReportSettings> insightsReportSetting, List<DataAdDimension> adDimensionList, out List<SavedReportDetails> submittedReports)
    {
        var idList = new List<string>();
        submittedReports = new List<SavedReportDetails>();

        var currentSnapshot = this.Snapshots.Find(r => r.QueueID == queueItem.ID);

        if (currentSnapshot == null)
        {
            var reportLevel = Utilities.UtilsText.ConvertToEnum<ListAsset>(insightsReportSetting.ReportSettings.Level);
            switch (reportLevel)
            {
                case ListAsset.Ad:
                    var adList = adDimensionList.Select(x => x.AdId).Distinct().ToList();
                    if (adList.Count != 0)
                        idList.AddRange(adList);
                    break;
                case ListAsset.AdSet:
                    var adSetList = adDimensionList.Select(x => x.AdSetId).Distinct().ToList();
                    if (adSetList.Count != 0)
                        idList.AddRange(adSetList);
                    break;
                case ListAsset.Campaign:
                    var campaignList = adDimensionList.Select(x => x.CampaignId).Distinct().ToList();
                    if (campaignList.Count != 0)
                        idList.AddRange(campaignList);
                    break;
                default:
                    break;
            }
            return idList;
        }

        var savedReportsToResubmit = currentSnapshot.ReportsToResubmit.Where(x => x.ReportName.Contains(insightsReportSetting.APIReportName));

        if (savedReportsToResubmit.Any())
        {
            idList.AddRange(savedReportsToResubmit.Select(x => x.EntityID));
        }

        var reportsToCheckStatus = currentSnapshot.ReportsToCheckStatus.Where(x => x.ReportName.Contains(insightsReportSetting.APIReportName));

        if (reportsToCheckStatus.Any())
        {
            submittedReports.AddRange(reportsToCheckStatus);
        }

        return idList;
    }

    #endregion

    #region ID Vault Actions

    public void SaveDownloadedDimensions(string accountId)
    {
        // update matching dimension vault
        var matchingVault = this.SavedIdVaults.Find(x => x.AccountID == accountId);

        if (matchingVault == null)
            return;

        // save info in s3
        if (this.SavedDimension.Exists)
            this.SavedDimension.Delete();

        // convert string to stream
        byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this.SavedIdVaults));

        using (MemoryStream stream = new MemoryStream(byteArray))
        {
            this.SavedDimension.Put(stream);
        }
    }

    public void LoadSavedIdVault(Policy downloadPolicy)
    {
        string content = string.Empty;

        downloadPolicy.Execute(() =>
        {
            if (this.SavedDimension.Exists)
            {
                using (var sr = new StreamReader(this.SavedDimension.Get()))
                {
                    content = sr.ReadToEnd();
                }
            }
        });

        this.SavedIdVaults = JsonConvert.DeserializeObject<List<IdVault>>(content) ?? new List<IdVault>();
    }

    /// <summary>
    /// Get only new IDs that have not already been downloaded
    /// </summary>
    /// <param name="accountId"></param>
    /// <param name="listAsset"></param>
    /// <param name="adIdTuple"></param>
    /// <returns></returns>
    public List<string> GetIdsForDimensionDownload(Queue queueItem, ListAsset listAsset)
    {
        var accountId = queueItem.EntityID.ToLower();

        var pendingIdList = new List<string>();
        var idsAlreadyRequested = new List<string>();
        var snapshotIdList = new List<string>();

        var matchingVault = this.SavedIdVaults.Find(x => x.AccountID == accountId);

        if (matchingVault != null)
        {
            var vaultIdList = matchingVault.GetDownloadedIdList(listAsset);
            idsAlreadyRequested.AddRange(vaultIdList);
        }

        var matchingSnapshot = this.Snapshots.Find(x => x.QueueID == queueItem.ID);

        if (matchingSnapshot != null)
        {
            var entityIdList = matchingSnapshot.GetDimensionIdList(listAsset);
            snapshotIdList.AddRange(entityIdList);
        }

        pendingIdList = snapshotIdList.Where(id => !idsAlreadyRequested.Contains(id)).ToList();

        if (matchingVault != null)
        {
            // add new dimension IDs to download complete
            matchingVault.AddOrUpdateList(ListType.DownloadComplete, listAsset, pendingIdList, 0);
        }
        else
        {
            var newVault = new IdVault { AccountID = accountId };

            // add new dimension IDs to download complete
            newVault.AddOrUpdateList(ListType.DownloadComplete, listAsset, pendingIdList, 0);

            this.SavedIdVaults.Add(newVault);
        }

        return pendingIdList;
    }

    #endregion
}

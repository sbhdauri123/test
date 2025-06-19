using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Twitter;
using Greenhouse.Data.DataSource.Twitter;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.GZip;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.Twitter;

[Export("Twitter-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private List<IFileItem> _queueItems;
    private IEnumerable<APIReport<ReportSettings>> _reports;
    private Auth.OAuthAuthenticator _oAuth;
    private string JobGUID => base.JED.JobGUID.ToString();
    private Uri _baseDestUri;
    private RemoteAccessClient _remoteS3AccessClient;
    private Uri _baseStageDestUri;
    private IBackOffStrategy _apiRetryBackOffStrategy;
    private ExponentialBackOffStrategy _downloadRetryBackOffStrategy;
    private Dictionary<string, List<APIReport<ReportSettings>>> _optInReportDictionary;
    private IEnumerable<APIReport<ReportSettings>> _allReportTypes;
    private List<string> _nonUsAccounts;

    private IEnumerable<APIEntity> _apiEntities;

    //key: queue.ID Value: list of reports for that queue
    private Dictionary<long, List<ApiReportItem>> _unfinishedReports = new();
    private List<ApiReportItem> _reportsMissingFromQueue = new();

    private Lookup _unfinishedReportLookup;
    private readonly Stopwatch _runtime = new();
    private TimeSpan _maxRuntime;
    private readonly List<long> _failedQueueIDs = new();
    private int _exceptionCounter;

    private const int EntityIdsBatchSize = 20;
    private const int JobIdsBatchSize = 200;
    private const int ActiveEntitiesBatchSize = 200;

    private IHttpClientProvider _httpClientProvider;
    private ApiClient _apiClient;
    private LoggerHandler _logger;

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;

        Stage = Constants.ProcessingStage.RAW;

        base.Initialize();
        SetupLogger();

        _oAuth = base.OAuthAuthenticator(version: "1.0");
        _remoteS3AccessClient = GetS3RemoteAccessClient();
        _baseDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseDestUri.ToString()
            .Replace(Constants.ProcessingStage.RAW.ToString().ToLower(),
                Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _nonUsAccounts =
            ETLProvider.DeserializeType<List<string>>(SetupService.GetById<Lookup>(Constants.TWITTER_NON_US_ACCOUNTS)
                .Value);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.TWITTER_MAX_RUNTIME, TimeSpan.FromHours(3));

        InitializeApiClient();
        LoadQueueItems();
        SetupReports();
        ConfigureBackOffStrategies();
        LoadUnfinishedReports();
    }

    public void Execute()
    {
        if (_queueItems.Count == 0)
        {
            _logger.LogNoReportsInQueue();
            return;
        }

        _logger.LogStartJobExecution();

        _runtime.Start();

        ProcessQueueItems();

        _runtime.Stop();

        if (HasRuntimeExceeded())
        {
            _logger.LogRuntimeExceeded(_runtime.ElapsedMilliseconds);
        }

        HandleExceptions();

        _logger.LogJobComplete();
        _logger.LogExecutionComplete();
    }

    private bool HasRuntimeExceeded()
    {
        bool hasRunTimeExceeded = _runtime.Elapsed > _maxRuntime;
        if (!hasRunTimeExceeded)
        {
            return false;
        }

        _logger.LogRuntimeExceeded(_runtime.ElapsedMilliseconds);
        return true;
    }

    private void ProcessQueueItems()
    {
        List<ApiReportItem> reportsToDownload = new();

        try
        {
            foreach (IFileItem queueItem in _queueItems)
            {
                if (HasRuntimeExceeded())
                {
                    break;
                }

                ProcessQueueItem(queueItem, reportsToDownload);
            }

            CheckStatusAndDownloadReport(_apiRetryBackOffStrategy, reportsToDownload);

            SaveUnfinishedReports(reportsToDownload);
        }
        catch (Exception ex)
        {
            _exceptionCounter++;
            _logger.LogGlobalException(ex);
        }
    }

    /// <summary>
    /// Asynchronously requests reports generation.
    /// <para>
    /// The processing rate limit is 100 concurrent jobs at a time.
    /// In theory, our retry mechanism should handle this scenario.
    /// If not, the unprocessed jobs will be picked up in the next run.
    /// </para>
    /// </summary>
    /// <see href="https://developer.x.com/en/docs/x-ads-api/analytics/overview"/>
    /// <param name="queueItem"></param>
    /// <param name="reportsToDownload"></param>
    private void ProcessQueueItem(IFileItem queueItem, List<ApiReportItem> reportsToDownload)
    {
        try
        {
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            if (HasUnfinishedReports(queueItem, reportsToDownload))
            {
                return;
            }

            List<APIReport<ReportSettings>> reportsToRequest = GetReportsToRequest(queueItem, reportsToDownload);

            ProcessReportsForQueue(queueItem, reportsToDownload, reportsToRequest);
        }
        catch (HttpClientProviderRequestException ex)
        {
            FailQueueItem(queueItem.ID);

            _logger.LogFailedReportGenerateReport(ex, queueItem);

            FailReportsForQueue(queueItem, reportsToDownload);
        }
        catch (Exception ex)
        {
            FailQueueItem(queueItem.ID);

            _logger.LogFailedReportGenerateReport(ex, queueItem);

            FailReportsForQueue(queueItem, reportsToDownload);
        }
    }

    private void ProcessReportsForQueue(IFileItem queueItem, List<ApiReportItem> reportList,
        List<APIReport<ReportSettings>> reportsToRequest)
    {
        TwitterRetry apiCallRetry = new(this.JobGUID, _apiRetryBackOffStrategy, _runtime,
            _maxRuntime);

        foreach (APIReport<ReportSettings> report in reportsToRequest)
        {
            ProcessReportForQueue(queueItem, reportList, report, apiCallRetry);
        }

        List<ApiReportItem> reports = FilterReportItemsByQueueID(reportList, queueItem.ID);
        HandleEmptyReports(queueItem, reports);
    }

    private void ProcessReportForQueue(IFileItem queueItem, List<ApiReportItem> reportList,
        APIReport<ReportSettings> report, TwitterRetry apiCallRetry)
    {
        ActiveEntities activeEntities = GetActiveEntities(queueItem, report, apiCallRetry);

        if (!HasActiveEntities(activeEntities, queueItem, report))
        {
            AddEmptyReportItem(queueItem, report, reportList);
            return;
        }

        ProcessReportByType(reportList, activeEntities, report, queueItem, apiCallRetry);
    }

    private bool HasActiveEntities(ActiveEntities activeEntities, IFileItem queueItem,
        APIReport<ReportSettings> report)
    {
        bool hasActiveEntities = activeEntities?.Data?.Count > 0;
        if (hasActiveEntities)
        {
            return true;
        }

        _logger.LogNoActiveEntitiesForEntity(queueItem, report.APIReportName);
        return false;
    }

    private void ProcessReportByType(List<ApiReportItem> reportList, ActiveEntities activeEntities,
        APIReport<ReportSettings> report, IFileItem queueItem, TwitterRetry apiCallRetry)
    {
        switch (report.ReportSettings.ReportType?.ToLower())
        {
            case ReportTypes.Dims:
                DownloadDimensionReports(reportList, activeEntities, report, queueItem, apiCallRetry);
                break;
            case ReportTypes.Stats:
                List<ApiReportItem> requestedReports = RequestReports(activeEntities, report, queueItem, apiCallRetry);
                reportList.AddRange(requestedReports);
                break;
        }
    }

    private void HandleEmptyReports(IFileItem queueItem, List<ApiReportItem> reportList)
    {
        foreach (ApiReportItem report in reportList.Where(report => report.HasNoActiveEntities))
        {
            CreateEmptyReport(queueItem, report);
        }
    }

    private bool ProcessStaticDimensionReports(IFileItem queueItem, List<ApiReportItem> reportList)
    {
        // download static dimension reports only once per entity
        ApiReportItem dimensionReportItem = reportList
            .Where(x => x.AccountID == queueItem.EntityID)
            .FirstOrDefault(x => x.StaticDimensionDownloaded);

        return dimensionReportItem != null || GetStaticDimensionReports(reportList, queueItem);
    }

    private bool AreAllReportsComplete(List<ApiReportItem> reports, IFileItem queueItem,
        ApiReportItem report = null)
    {
        _logger.LogMarkAsCompleteRecordsWithNoData();

        long queueId = report?.QueueID ?? queueItem.ID;

        bool areAllReportsComplete = reports
            .Where(x => x.QueueID == queueId && !x.IsStaticDimension)
            .All(x => x.IsDownloaded && x.IsReady);

        if (!areAllReportsComplete)
        {
            return false;
        }

        _logger.LogEntityDrivenReportsComplete(queueItem);
        return true;
    }

    /// <summary>
    /// Synchronous download of entities
    /// </summary>
    private void DownloadDimensionReports(
        List<ApiReportItem> reportList,
        ActiveEntities activeEntities,
        APIReport<ReportSettings> report,
        IFileItem queueItem,
        TwitterRetry twitterApiCallRetry)
    {
        IEnumerable<IEnumerable<AdsActiveEntity>> activeEntitiesBatch =
            Utilities.UtilsText.GetSublistFromList(activeEntities.Data, ActiveEntitiesBatchSize);

        foreach (IEnumerable<AdsActiveEntity> batch in activeEntitiesBatch)
        {
            ApiReportItem reportItem = new()
            {
                QueueID = queueItem.ID,
                FileGuid = queueItem.FileGUID,
                ReportName = report.APIReportName,
                AccountID = queueItem.EntityID,
                IsReady = true
            };

            DownloadDimensionReport(report, queueItem, twitterApiCallRetry, batch.Select(x => x.EntityId).ToList(),
                reportItem);

            reportList.Add(reportItem);
        }
    }

    private static void AddEmptyReportItem(IFileItem queueItem, APIReport<ReportSettings> report,
        List<ApiReportItem> reportList)
    {
        ApiReportItem reportItem = new()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ReportID = "0",
            ReportName = report.APIReportName,
            AccountID = queueItem.EntityID,
            StartTime = queueItem.FileDate,
            EndTime = queueItem.FileDate.AddDays(1),
            ReportURL = "",
            Placement = "",
            Status = "",
            IsReady = true,
            IsDownloaded = true,
            HasNoActiveEntities = true
        };

        reportList.Add(reportItem);
    }

    private ActiveEntities GetActiveEntities(IFileItem queueItem, APIReport<ReportSettings> report,
        TwitterRetry twitterApiCallRetry)
    {
        if (report.ReportSettings.IsStaticDimension)
        {
            return new ActiveEntities();
        }

        if (report.ReportSettings.Entity.Equals(Entities.Account, StringComparison.CurrentCultureIgnoreCase))
        {
            return new ActiveEntities { Data = new List<AdsActiveEntity> { new() { EntityId = queueItem.EntityID } } };
        }

        return twitterApiCallRetry.Execute(() =>
        {
            GetActiveEntitiesOptions options = new()
            {
                AccountId = queueItem.EntityID,
                Entity = report.ReportSettings.Entity,
                FileDate = queueItem.FileDate,
                Granularity = report.ReportSettings.Granularity
            };

            return _apiClient.GetActiveEntitiesAsync(options).GetAwaiter().GetResult();
        });
    }

    private List<APIReport<ReportSettings>> GetReportsToRequest(IFileItem queueItem, List<ApiReportItem> reportList)
    {
        List<APIReport<ReportSettings>> reportsToRequest = new();

        foreach (APIReport<ReportSettings> report in _reports)
        {
            if (ShouldSkipReport(report, queueItem))
            {
                continue;
            }

            if (report.ReportSettings.IsStaticDimension)
            {
                reportList.Add(new ApiReportItem()
                {
                    QueueID = queueItem.ID,
                    FileGuid = queueItem.FileGUID,
                    ReportName = report.APIReportName,
                    AccountID = queueItem.EntityID,
                    IsReady = true,
                    IsStaticDimension = true,
                    ApiReport = report
                });
                continue;
            }

            reportsToRequest.Add(report);
        }

        if (_optInReportDictionary.TryGetValue(queueItem.EntityID,
                out List<APIReport<ReportSettings>> optInReports))
        {
            reportsToRequest.AddRange(optInReports);
        }

        return reportsToRequest;
    }

    private void CreateEmptyReport(IFileItem queueItem, ApiReportItem report)
    {
        string fileName = $"{queueItem.FileGUID}_{report.ReportName}_{queueItem.FileName}_0.json";
        string[] paths = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName };

        S3File rawFile = new(RemoteUri.CombineUri(this._baseDestUri, paths), GreenhouseS3Creds);
        Stream rawFileStream = rawFile.Create();
        rawFileStream.Close();

        List<FileCollectionItem> files = queueItem.FileCollection?.ToList() ?? new List<FileCollectionItem>();

        FileCollectionItem fileItem = new()
        {
            FileSize = rawFile.Length,
            SourceFileName = report.ReportName,
            FilePath = fileName
        };

        files.Add(fileItem);

        queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
        queueItem.FileSize += rawFile.Length;
        queueItem.DeliveryFileDate =
            UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, rawFile.LastWriteTimeUtc);
    }

    // if segmenting per country, only allow US accounts
    private bool ShouldSkipReport(APIReport<ReportSettings> report, IFileItem queueItem)
    {
        if (report.ReportSettings.SegmentationType == SegmentationTypes.Country &&
            _nonUsAccounts.Contains(queueItem.EntityID))
        {
            _logger.LogAccountIsNonUS(queueItem.EntityID, report.APIReportName);
            return true;
        }

        return false;
    }

    private bool HasUnfinishedReports(IFileItem queueItem, List<ApiReportItem> reports)
    {
        bool hasUnfinishedReports = _unfinishedReports.Any(r => r.Key == queueItem.ID);
        if (!hasUnfinishedReports)
        {
            return false;
        }

        reports.AddRange(_unfinishedReports.Where(r => r.Key == queueItem.ID)
            .SelectMany(r => r.Value));

        return true;
    }

    private bool GetStaticDimensionReports(List<ApiReportItem> reportList, IFileItem queueItem)
    {
        bool returnVal;
        try
        {
            List<ApiReportItem> staticDimensionReports =
                reportList.Where(x => x.QueueID == queueItem.ID && x.IsStaticDimension).ToList();

            TwitterRetry twitterApiCallRetry = new(this.JobGUID, _apiRetryBackOffStrategy, _runtime, _maxRuntime);

            foreach (ApiReportItem report in staticDimensionReports)
            {
                DownloadDimensionReport(report.ApiReport, queueItem, twitterApiCallRetry, null,
                    report);

                report.IsDownloaded = true;

                bool allStaticDimDownloaded = staticDimensionReports.All(x => x.IsDownloaded && x.IsReady);
                if (allStaticDimDownloaded)
                {
                    report.StaticDimensionDownloaded = true;
                }
            }

            returnVal = true;
        }
        catch (HttpClientProviderRequestException exc)
        {
            returnVal = false;
            FailQueueItem(queueItem.ID);
            _logger.LogErrorDownloadingDimensionReport(exc, queueItem);
        }
        catch (Exception exc)
        {
            returnVal = false;
            FailQueueItem(queueItem.ID);
            _logger.LogErrorDownloadingDimensionReport(exc, queueItem);
        }

        return returnVal;
    }

    private static string BuildDimensionReportFileName(IFileItem queueItem, APIReport<ReportSettings> report,
        int pageNumber)
        => $"{queueItem.FileGUID}_{report.APIReportName}_{queueItem.FileName}_{pageNumber}.json";


    private void DownloadDimensionReport(
        APIReport<ReportSettings> report, IFileItem queueItem, TwitterRetry twitterApiCallRetry,
        List<string> entityIds, ApiReportItem reportItem)
    {
        int pageNumber = 0;
        string cursor = string.Empty;

        do
        {
            string fileName = BuildDimensionReportFileName(queueItem, report, pageNumber);
            DownloadDimensionFileOptions options = new()
            {
                EntityIds = entityIds,
                Cursor = cursor,
                Report = report,
                AccountId = queueItem.EntityID,
            };

            S3File rawFile = twitterApiCallRetry.Execute(() =>
            {
                using Stream responseStream = _apiClient.DownloadDimensionFileAsync(options).GetAwaiter().GetResult();
                using StreamReader reader = new(responseStream);
                string content = reader.ReadToEndAsync().GetAwaiter().GetResult();

                // later on, DimensionReport will be used to extract Data from the report, 
                // for each report the specific class matching its properties will be specified. 
                // Here we are just interested in getting the property NextCursor, so a generic
                // object is specified
                DimensionReport<object> dimensionReport =
                    JsonConvert.DeserializeObject<DimensionReport<object>>(content);
                cursor = dimensionReport.NextCursor;

                if (report.ReportSettings.ReportType?.ToLower() == ReportTypes.Dims &&
                    report.ReportSettings.Entity?.ToUpper() == Entities.Account)
                {
                    UpdateTimezoneIfNeeded(dimensionReport, queueItem);
                }

                string[] paths = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName };

                S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
                StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
                UploadToS3(incomingFile, rawFile, paths);

                UpdateReportItem(reportItem, rawFile, report.APIReportName);
                AddToQueueFileCollection(queueItem, reportItem);

                return rawFile;
            });

            _logger.LogSyncDownloadReportEnd(queueItem, cursor, fileName, CurrentSource.SourceName);
            pageNumber++;
        } while (!string.IsNullOrEmpty(cursor));

        reportItem.IsDownloaded = true;
    }

    private void UpdateTimezoneIfNeeded(DimensionReport<object> dimensionReport, IFileItem queueItem)
    {
        if (dimensionReport.Data.FirstOrDefault() is not JObject firstDataItem)
        {
            return;
        }

        string timezone = firstDataItem["timezone"]?.Value<string>();
        if (string.IsNullOrEmpty(timezone))
        {
            return;
        }

        APIEntity apiEntity = _apiEntities.FirstOrDefault(a => a.APIEntityCode == queueItem.EntityID);
        if (apiEntity == null || apiEntity.TimeZone == timezone)
        {
            return;
        }

        apiEntity.TimeZone = timezone;
        apiEntity.LastUpdated = DateTime.Now;
        SetupService.Update(apiEntity);
    }

    private static void UpdateReportItem(ApiReportItem reportItem, S3File rawFile, string reportName)
    {
        reportItem.FileCollectionItem = new FileCollectionItem
        {
            FileSize = rawFile.Length,
            SourceFileName = reportName,
            FilePath = rawFile.Name
        };
        reportItem.LastWriteTimeUtc = rawFile.LastWriteTimeUtc;
    }

    private void StageReport(IFileItem queueItem, List<ApiReportItem> reports)
    {
        try
        {
            if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
            {
                _logger.LogUnableToStageData(queueItem);
                return;
            }

            IEnumerable<FileCollectionItem> reportFileList = queueItem.FileCollection;
            List<FileCollectionItem> staticDimensionReports = new();

            //use counter to make stage file name unique
            int stagedReportCount = 0;

            foreach (FileCollectionItem reportFile in reportFileList)
            {
                APIReport<ReportSettings> reportType = GetReportType(reportFile);

                // skip staging static dimension
                if (reportType.ReportSettings.IsStaticDimension)
                {
                    staticDimensionReports.Add(reportFile);
                    continue;
                }

                StageReportsByType(queueItem, reportFile, reportType, reports, ref stagedReportCount);
                stagedReportCount++;
            }

            StageStaticDimensionReports(queueItem, staticDimensionReports);

            JobStatusComplete(queueItem, stagedReportCount);
        }
        catch (HttpClientProviderRequestException ex)
        {
            FailQueueItem(queueItem.ID);
            _logger.LogErrorStagingDataInS3(ex, queueItem);
        }
        catch (Exception ex)
        {
            FailQueueItem(queueItem.ID);
            _logger.LogErrorStagingDataInS3(ex, queueItem);
        }
    }

    private void StageStaticDimensionReports(IFileItem queueItem, List<FileCollectionItem> staticDimensionReports)
    {
        List<IGrouping<string, FileCollectionItem>> staticReportsByType =
            staticDimensionReports.GroupBy(r => r.SourceFileName).ToList();

        foreach (IGrouping<string, FileCollectionItem> reportGroup in staticReportsByType)
        {
            CreateManifestFile((Queue)queueItem, reportGroup.ToList(), reportGroup.Key.ToLower());
        }
    }

    private void JobStatusComplete(IFileItem queueItem, int stagedReportCount)
    {
        _logger.LogChangingQueueStatusToComplete(queueItem, stagedReportCount);
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update((Queue)queueItem);
    }

    private void StageReportsByType(IFileItem queueItem, FileCollectionItem reportFile,
        APIReport<ReportSettings> reportType, List<ApiReportItem> reports, ref int stagedReportCount)
    {
        if (reportType.ReportSettings.ReportType == ReportTypes.Dims)
        {
            StageDimensionsReport(queueItem, reportFile, WriteObjectToFile, stagedReportCount, reportType);
        }
        else if (reportType.ReportSettings.ReportType == ReportTypes.Stats)
        {
            StageMetricsReport(queueItem, reportFile, WriteObjectToFile, stagedReportCount, reportType, reports);
        }
    }

    private APIReport<ReportSettings> GetReportType(FileCollectionItem reportFile)
    {
        return _allReportTypes.FirstOrDefault(x =>
            x.APIReportName.Equals(reportFile.SourceFileName,
                StringComparison.InvariantCultureIgnoreCase));
    }

    private void WriteObjectToFile(JArray entity, string entityId, DateTime fileDate, string filename)
    {
        string[] paths = { entityId.ToLower(), GetDatedPartition(fileDate), filename };

        IFile transformedFile = _remoteS3AccessClient.WithFile(RemoteUri.CombineUri(_baseStageDestUri, paths));
        ETLProvider.SerializeRedshiftJson(entity, transformedFile);
    }

    private void StageMetricsReport(IFileItem queueItem, FileCollectionItem file,
        Action<JArray, string, DateTime, string> writeToFileSignature, int counter,
        APIReport<ReportSettings> reportType, List<ApiReportItem> reports)
    {
        List<RootStatsJob> reportData = GetReportData<RootStatsJob>(file.FilePath, queueItem);
        string fileName = $"{queueItem.FileGUID}_{file.SourceFileName}_{{0}}_{counter}.json";
        List<string> metricGroups = reportType.ReportSettings.MetricGroups.Split(',').ToList();

        _logger.LogStagingMetricsReport(queueItem, file, counter, reportType.ReportSettings.Entity);

        ApiReportItem report = reports.FirstOrDefault(r =>
            r.QueueID == queueItem.ID &&
            r.FileCollectionItem?.SourceFileName == file.SourceFileName);

        foreach (string metricGroup in metricGroups)
        {
            StageMetricsGroup(queueItem, file, writeToFileSignature, reportType, report, metricGroup, reportData,
                fileName);
        }
    }

    private static void StageMetricsGroup(IFileItem queueItem, FileCollectionItem file,
        Action<JArray, string, DateTime, string> writeToFileSignature,
        APIReport<ReportSettings> reportType, ApiReportItem report, string metricGroup,
        List<RootStatsJob> reportData, string fileName)
    {
        switch (metricGroup.ToUpper())
        {
            case MetricGroups.Engagement:
                TwitterService.StageEngagementMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings, reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
            case MetricGroups.Billing:
                TwitterService.StageBillingMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings, reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
            case MetricGroups.Video:
                TwitterService.StageVideoMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings, reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
            case MetricGroups.Media:
                TwitterService.StageMediaMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings, reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
            case MetricGroups.WebConversion:
                TwitterService.StageWebConversionMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings, reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
            case MetricGroups.MobileConversion:
                TwitterService.StageMobileConversionMetric(queueItem.FileGUID.ToString(), file.SourceFileName,
                    reportType.ReportSettings,
                    reportData,
                    queueItem.EntityID, queueItem.FileDate, writeToFileSignature, fileName, report);
                break;
        }
    }

    private void StageDimensionsReport(IFileItem queueItem, FileCollectionItem report,
        Action<JArray, string, DateTime, string> writeToFileSignature, int counter,
        APIReport<ReportSettings> reportType)
    {
        string fileName = $"{queueItem.FileGUID}_{report.SourceFileName}_{counter}.json";

        _logger.LogStagingDimensionReport(report, queueItem, counter, reportType.ReportSettings.Entity);

        switch (reportType.ReportSettings.Entity.ToUpper())
        {
            case Entities.Account:
                var fullAccountData =
                    GetReportData<DimensionReport<AccountDimensionReport>>(report.FilePath, queueItem);
                TwitterService.StageAccounts(queueItem.EntityID, queueItem.FileDate, fullAccountData, fileName,
                    writeToFileSignature);
                break;
            case Entities.Campaign:
                var fullCampaignData =
                    GetReportData<DimensionReport<CampaignDimensionReport>>(report.FilePath, queueItem);
                TwitterService.StageCampaigns(queueItem.EntityID, queueItem.FileDate, fullCampaignData, fileName,
                    writeToFileSignature);
                break;
            case Entities.LineItem:
                var fullLineItemData =
                    GetReportData<DimensionReport<LineItemDimensionReport>>(report.FilePath, queueItem);
                TwitterService.StageLineItem(queueItem.EntityID, queueItem.FileDate, fullLineItemData, fileName,
                    writeToFileSignature);
                break;
            case Entities.PromotedTweet:
                var fullPromotedTweetData =
                    GetReportData<DimensionReport<PromotedTweetDimensionReport>>(report.FilePath, queueItem);
                TwitterService.StagePromotedTweet(queueItem.EntityID, queueItem.FileDate, fullPromotedTweetData,
                    fileName,
                    writeToFileSignature);
                break;
            case Entities.MediaCreative:
                var fullMediaCreativeData =
                    GetReportData<DimensionReport<MediaCreativeDimensionReport>>(report.FilePath, queueItem);
                TwitterService.StageMediaCreative(queueItem.EntityID, queueItem.FileDate, fullMediaCreativeData,
                    fileName,
                    writeToFileSignature);
                break;
        }
    }

    private List<T> GetReportData<T>(string report, IFileItem queueItem) where T : new()
    {
        List<T> statsData = new();

        string[] paths = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report };

        Uri s3ReportFilePath = RemoteUri.CombineUri(this._baseDestUri, paths);

        //todo check for s3 file size
        Stream s3ReportFileStream = _remoteS3AccessClient.WithFile(s3ReportFilePath).Get();

        if (report.EndsWith(".gz"))
        {
            using GZipInputStream zipStream = new(s3ReportFileStream);
            List<T> rootStatsJob = ETLProvider.DeserializeJSONStream(new T(), zipStream);
            statsData.AddRange(rootStatsJob);
        }
        else
        {
            using StreamReader txtReader = new(s3ReportFileStream);
            T rootStatsJob = JsonConvert.DeserializeObject<T>(txtReader.ReadToEnd());
            if (null != rootStatsJob)
            {
                statsData.Add(rootStatsJob);
            }
        }

        return statsData;
    }

    private void SaveUnfinishedReports(List<ApiReportItem> reportList)
    {
        // we are saving all reports not downloaded unless the queue was marked as error (in that case all reports need to be requested again)
        List<ApiReportItem> notReadyList = reportList.Where(x =>
            !x.IsDownloaded && x.Status != ReportStatus.Cancelled && !_failedQueueIDs.Contains(x.QueueID)).ToList();

        List<ApiReportItem> allReports = new();

        // if even 1 report is not ready for a queue, we are saving all reports for that queue
        // as those reports will be staged together when they are all ready
        List<long> unfinishedQueues = notReadyList.Select(r => r.QueueID).Distinct().ToList();
        foreach (long queueId in unfinishedQueues)
        {
            JobService.UpdateQueueStatus(queueId, Constants.JobStatus.Pending);

            _logger.LogReportsNotReadyForQueue(queueId);

            // for any unfinished report we are also passing along all the other reports associated with the queue, as they will all be staged together
            // when all are ready
            allReports.AddRange(reportList.Where(x => x.QueueID == queueId));
        }

        // we are going to store all the unfinished reports in Lookup
        // all unfinished reports = unfinished reports not imported this time because their queues was not in the selection + newly unfinished reports

        // first cleaning up the list in reportsMissingFromQueue by making sure they still have a queue in the Queue table (if a queue was manually deleted the associated reports should be removed from the list)
        List<long> queueIdList = JobService.GetQueueIDBySource(base.SourceId)?.ToList();
        _reportsMissingFromQueue = _reportsMissingFromQueue
            .Where(r => queueIdList != null && queueIdList.Contains(r.QueueID)).ToList();

        allReports.AddRange(_reportsMissingFromQueue);

        if (_unfinishedReportLookup == null)
        {
            _unfinishedReportLookup = new Lookup()
            {
                Name = $"{Constants.TWITTER_UNFINISHED_REPORTS}_{CurrentIntegration.IntegrationID}",
                CreatedDate = DateTime.Now,
                IsEditable = true
            };
        }

        // storing the reports as a dictionary (per queue) to make it easier to edit manually (to remove all the reports of a queue for example)
        Dictionary<long, List<ApiReportItem>> unfinishedDictionary = allReports.GroupBy(r => r.QueueID)
            .ToDictionary(r => r.Key, r => r.ToList());

        _unfinishedReportLookup.Value = JsonConvert.SerializeObject(unfinishedDictionary);
        _unfinishedReportLookup.LastUpdated = DateTime.Now;

        LookupRepository.AddOrUpdateLookup(_unfinishedReportLookup);
    }

    private void FailQueueItem(long queueId)
    {
        Queue queueItem = (Queue)_queueItems.FirstOrDefault(q => q.ID == queueId);
        if (queueItem == null)
        {
            return;
        }

        queueItem.StatusId = (int)Constants.JobStatus.Error;
        queueItem.Status = Constants.JobStatus.Error.ToString();
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        _exceptionCounter++;

        if (!_failedQueueIDs.Contains(queueItem.ID))
        {
            _failedQueueIDs.Add(queueItem.ID);
        }
    }

    private void CheckStatusAndDownloadReport(IBackOffStrategy apiCallsBackOffStrategy,
        List<ApiReportItem> allReports)
    {
        try
        {
            if (allReports.Count == 0)
            {
                _logger.LogNoReportsToRun();
                return;
            }

            TwitterRetry twitterApiCallRetry = new(this.JobGUID, apiCallsBackOffStrategy, _runtime, _maxRuntime);

            CancellableConditionalRetry<bool> allReportReadyRetry = CreateAllReportReadyRetry();

            bool allDone = allReportReadyRetry.Execute(() => ProcessAllReports(allReports, twitterApiCallRetry));

            if (!allDone)
            {
                throw new APIReportException(
                    $"ReportStatus-check attempts exceeded the max of {_downloadRetryBackOffStrategy.MaxRetry}");
            }

            foreach (IFileItem queueItem in _queueItems)
            {
                List<ApiReportItem> reports = FilterReportItemsByQueueID(allReports, queueItem.ID);
                if (!AreAllReportsReadyToStage(reports))
                {
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                    continue;
                }

                StageReport(queueItem, reports);
            }
        }
        catch (Exception exception)
        {
            _exceptionCounter++;
            _logger.LogCheckStatusAndDownloadReportException(exception);
        }
    }

    private static List<ApiReportItem> FilterReportItemsByQueueID(List<ApiReportItem> allReports, long queueID)
    {
        return allReports.Where(report => report.QueueID == queueID).ToList();
    }

    private static bool AreAllReportsReadyToStage(List<ApiReportItem> reportList)
    {
        return reportList.All(report => report.IsReady && report.IsDownloaded);
    }

    private CancellableConditionalRetry<bool> CreateAllReportReadyRetry()
    {
        return new CancellableConditionalRetry<bool>(
            this.JED.JobGUID.ToString(),
            _downloadRetryBackOffStrategy,
            _runtime,
            _maxRuntime,
            (bool allReportsDone) => !allReportsDone);
    }

    private bool ProcessAllReports(List<ApiReportItem> reportList, TwitterRetry twitterApiCallRetry)
    {
        List<ApiReportItem> unprocessedReports = GetUnprocessedReports(reportList);
        _logger.LogReportsNotReadyToDownload(unprocessedReports.Count, reportList.Count);

        IEnumerable<IGrouping<string, ApiReportItem>> reportsByAccount = GroupReportsByAccount(unprocessedReports);

        foreach (IGrouping<string, ApiReportItem> accountReports in reportsByAccount)
        {
            ProcessReportsForAccount(accountReports.Key, accountReports, twitterApiCallRetry, reportList);
        }

        SkipFailedReports(reportList);

        return AreAllReportsReady(reportList);
    }

    private void ProcessReportsForAccount(string accountId, IEnumerable<ApiReportItem> accountReports,
        TwitterRetry twitterApiCallRetry, List<ApiReportItem> allReports)
    {
        IEnumerable<IEnumerable<string>> reportIdBatches = GetAccountReportIdsInBatches(accountReports);

        foreach (IEnumerable<string> batch in reportIdBatches)
        {
            if (HasRuntimeExceeded())
            {
                _logger.LogRuntimeExceeded(_runtime.ElapsedMilliseconds);
                throw new AllotedRunTimeExceededException(
                    $"The runtime ({_runtime}) exceeded the allotted time {_maxRuntime}");
            }

            ProcessReportBatch(accountId, batch.ToList(), twitterApiCallRetry, allReports);
        }
    }

    private void ProcessReportBatch(string accountId, List<string> batch, TwitterRetry twitterApiCallRetry,
        List<ApiReportItem> allReports)
    {
        try
        {
            List<string> validBatch = RemoveCancelledReports(batch, allReports);
            if (validBatch.Count == 0)
            {
                _logger.LogSkippingReportStatusCheckAllReportsFailed();
                return;
            }

            ReportRequestStatusResponse apiReport =
                CheckReportRequestStatus(accountId, validBatch, twitterApiCallRetry);

            UpdateAndDownloadReports(apiReport.Data, allReports, twitterApiCallRetry);
            HandleMissingReports(validBatch, apiReport.Data, allReports);
        }
        catch (Exception ex)
        {
            _logger.FailedReportStatusCheckException(ex, accountId);
            CancelReportsInBatch(batch, allReports);
        }
    }


    private static List<string> RemoveCancelledReports(IEnumerable<string> batch, List<ApiReportItem> allReports)
    {
        return batch.Where(reportId =>
            !allReports.Any(x => x.ReportID == reportId && x.Status == ReportStatus.Cancelled)
        ).ToList();
    }

    private ReportRequestStatusResponse CheckReportRequestStatus(string accountId, List<string> batch,
        TwitterRetry twitterApiCallRetry)
    {
        return twitterApiCallRetry.Execute(() =>
            _apiClient.GetReportRequestStatusAsync(
                    new GetReportRequestStatusOptions { AccountId = accountId, JobIds = batch }).GetAwaiter()
                .GetResult());
    }

    private void UpdateAndDownloadReports(List<ReportRequestData> readyReportList, List<ApiReportItem> allReports,
        TwitterRetry twitterApiCallRetry)
    {
        foreach (ReportRequestData readyReport in readyReportList)
        {
            ApiReportItem report = allReports.FirstOrDefault(r => r.ReportID == readyReport.Id);
            if (report == null)
            {
                _logger.LogReportNoFound(readyReport.Id);
                continue;
            }

            UpdateReportStatus(report, readyReport);

            if (report.Status == ReportStatus.Success && !_failedQueueIDs.Contains(report.QueueID))
            {
                DownloadSuccessfulReport(report, allReports, twitterApiCallRetry);
            }
            else if (report.Status != ReportStatus.Processing)
            {
                CancelReport(report);
            }
        }
    }

    private static void UpdateReportStatus(ApiReportItem report, ReportRequestData readyReport)
    {
        report.Status = readyReport.Status;
        report.IsReady = true;
        report.ReportURL = readyReport.UrlAddress;
    }

    private void DownloadSuccessfulReport(ApiReportItem report, List<ApiReportItem> allReports,
        TwitterRetry twitterApiCallRetry)
    {
        IFileItem queueItem = _queueItems.FirstOrDefault(x => x.ID == report.QueueID);
        if (queueItem == null)
        {
            _logger.LogReportNoFound(report.ReportID);
            return;
        }

        report.IsDownloaded = DownloadReport(report, queueItem, twitterApiCallRetry);

        if (report.IsDownloaded)
        {
            if (!AreAllReportsComplete(allReports, queueItem, report))
            {
                return;
            }

            ProcessStaticDimensionReports(queueItem, allReports);
        }
        else
        {
            CancelReport(report);
        }
    }

    private void CancelReport(ApiReportItem report)
    {
        report.Status = ReportStatus.Cancelled;
        FailQueueItem(report.QueueID);
    }

    private void HandleMissingReports(List<string> requestedReportIds, List<ReportRequestData> apiReports,
        List<ApiReportItem> allReports)
    {
        List<string> apiReportIds = apiReports.Select(x => x.Id).ToList();
        List<string> missingReportIds = requestedReportIds.Except(apiReportIds).ToList();

        if (missingReportIds.Count == 0)
        {
            return;
        }

        _logger.LogMissingReports(missingReportIds);

        foreach (string missingReportId in missingReportIds)
        {
            ApiReportItem report = allReports.FirstOrDefault(r => r.ReportID == missingReportId);
            if (report != null)
            {
                CancelReport(report);
            }
        }
    }

    private void CancelReportsInBatch(List<string> batch, List<ApiReportItem> allReports)
    {
        foreach (string reportId in batch)
        {
            ApiReportItem report = allReports.FirstOrDefault(x => x.ReportID == reportId);
            if (report == null)
            {
                continue;
            }

            CancelReport(report);
            _logger.LogFailedReportStatusCheck(report.FileGuid, report.ReportName,
                reportId);
        }
    }

    private static bool AreAllReportsReady(List<ApiReportItem> reportList)
    {
        return reportList.All(x => x.IsReady);
    }

    private static IEnumerable<IEnumerable<string>> GetAccountReportIdsInBatches(
        IEnumerable<ApiReportItem> accountReports)
    {
        return UtilsText.GetSublistFromList(
            accountReports.OrderByDescending(x => x.QueueID).Select(x => x.ReportID),
            JobIdsBatchSize
        );
    }

    private static IEnumerable<IGrouping<string, ApiReportItem>> GroupReportsByAccount(
        List<ApiReportItem> unprocessedReports)
    {
        return unprocessedReports.GroupBy(x => x.AccountID);
    }

    private static List<ApiReportItem> GetUnprocessedReports(List<ApiReportItem> reportList)
    {
        return reportList.Where(x => !x.IsDownloaded && !x.IsStaticDimension).ToList();
    }

    private void SkipFailedReports(List<ApiReportItem> reportList)
    {
        HashSet<long> failedQueueIds = GetFailedQueueIds(reportList);
        List<ApiReportItem> reportsToSkip = GetReportsToSkip(reportList, failedQueueIds);

        foreach (ApiReportItem report in reportsToSkip)
        {
            MarkReportAsSkipped(report);
        }

        LogSkippedReports(reportsToSkip);
    }

    private static HashSet<long> GetFailedQueueIds(List<ApiReportItem> reportList)
    {
        return new HashSet<long>(reportList
            .Where(x => x.Status == ReportStatus.Cancelled)
            .Select(x => x.QueueID));
    }

    private static List<ApiReportItem> GetReportsToSkip(List<ApiReportItem> reportList,
        HashSet<long> failedQueueIds)
    {
        return reportList.Where(x => !x.IsReady && failedQueueIds.Contains(x.QueueID)).ToList();
    }

    private static void MarkReportAsSkipped(ApiReportItem report)
    {
        report.IsReady = true;
        report.Status = ReportStatus.Cancelled;
    }

    private void LogSkippedReports(IEnumerable<ApiReportItem> skippedReports)
    {
        foreach (ApiReportItem report in skippedReports)
        {
            _logger.LogSkippingReportStatusCheck(report.FileGuid, report.ReportName);
        }
    }

    private static void FailReportsForQueue(IFileItem queue, List<ApiReportItem> reportList)
    {
        IEnumerable<ApiReportItem> failedReports = reportList.Where(x => x.QueueID == queue.ID);

        foreach (ApiReportItem reportItem in failedReports)
        {
            reportItem.IsReady = true;
            reportItem.Status = ReportStatus.Cancelled;
        }
    }

    private bool DownloadReport(ApiReportItem reportItem, IFileItem queueItem, TwitterRetry twitterApiCallRetry)
    {
        bool returnVal;
        try
        {
            string fileName =
                $"{queueItem.FileGUID}_{reportItem.ReportName}_{queueItem.FileName}_{reportItem.ReportID}.json.gz";

            _logger.LogStartDownloadReport(queueItem, reportItem, fileName, CurrentSource.SourceName);

            twitterApiCallRetry.Execute(() =>
            {
                DownloadReportOptions options = new()
                {
                    AccountId = queueItem.EntityID,
                    ReportUrl = reportItem.ReportURL
                };

                using Stream responseStream = _apiClient.DownloadReportFileAsync(options).GetAwaiter().GetResult();
                string[] paths = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName };

                S3File rawFile = new(RemoteUri.CombineUri(this._baseDestUri, paths),
                    GreenhouseS3Creds);

                StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
                base.UploadToS3(incomingFile, rawFile, paths);

                FileCollectionItem fileItem = new()
                {
                    FileSize = rawFile.Length,
                    SourceFileName = reportItem.ReportName,
                    FilePath = fileName
                };

                reportItem.FileCollectionItem = fileItem;
                reportItem.LastWriteTimeUtc = rawFile.LastWriteTimeUtc;
                AddToQueueFileCollection(queueItem, reportItem);

                return rawFile;
            });

            returnVal = true;

            _logger.LogDownloadReportEnd(queueItem, reportItem, fileName, CurrentSource.SourceName);
        }
        catch (HttpClientProviderRequestException exc)
        {
            returnVal = false;
            FailQueueItem(reportItem.QueueID);
            _logger.LogErrorDownloadingReport(exc, reportItem);
        }
        catch (Exception exc)
        {
            returnVal = false;
            FailQueueItem(reportItem.QueueID);
            _logger.LogErrorDownloadingReport(exc, reportItem);
        }

        return returnVal;
    }

    private static void AddToQueueFileCollection(IFileItem queueItem, ApiReportItem apiReportItem)
    {
        if (apiReportItem.FileCollectionItem == null)
        {
            return;
        }

        List<FileCollectionItem> files = queueItem.FileCollection?.ToList() ?? new List<FileCollectionItem>();
        files.Add(apiReportItem.FileCollectionItem);
        queueItem.FileCollectionJSON = JsonConvert.SerializeObject(files);
        queueItem.FileSize += apiReportItem.FileCollectionItem.FileSize;
        queueItem.DeliveryFileDate =
            UtilsDate.GetLatestDateTime(queueItem.DeliveryFileDate, apiReportItem.LastWriteTimeUtc);
    }

    private FileCollectionItem CreateManifestFile(Queue queueWithData, List<FileCollectionItem> fileItems,
        string fileType, Queue currentQueue = null)
    {
        currentQueue ??= queueWithData;

        RedshiftManifest manifest = new();
        PopulateManifest(manifest, queueWithData, fileItems);

        string fileName = GenerateManifestFileName(currentQueue, fileType);
        string[] manifestPath = GetManifestFilePath(currentQueue, fileName);

        string manifestFilePath = ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);

        return CreateFileCollectionItem(fileItems, fileType, fileName);
    }

    private void PopulateManifest(RedshiftManifest manifest, Queue queueWithData, List<FileCollectionItem> fileItems)
    {
        foreach (FileCollectionItem file in fileItems)
        {
            string s3File = GenerateS3FilePath(queueWithData, file);
            manifest.AddEntry(s3File, true);
        }
    }

    private string GenerateS3FilePath(Queue queueWithData, FileCollectionItem file)
    {
        return
            $"{this._baseDestUri.OriginalString.TrimStart('/')}/{queueWithData.EntityID.ToLower()}/{GetDatedPartition(queueWithData.FileDate)}/{file.FilePath}";
    }

    private static string GenerateManifestFileName(Queue queue, string fileType) =>
        $"{queue.FileGUID}_{fileType}.manifest";

    private static FileCollectionItem CreateFileCollectionItem(List<FileCollectionItem> fileItems, string fileType,
        string fileName)
    {
        return new FileCollectionItem
        {
            FileSize = fileItems.Sum(file => file.FileSize),
            SourceFileName = fileType,
            FilePath = fileName
        };
    }

    private string[] GetManifestFilePath(Queue queueItem, string name)
    {
        string[] partitionPath = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };

        Uri manifestUri = RemoteUri.CombineUri(_baseStageDestUri, partitionPath);
        return new[] { manifestUri.AbsolutePath, name };
    }

    private Dictionary<string, List<APIReport<ReportSettings>>> GetOptInReports(ApiReportLookup optInReportLookup,
        out List<APIReport<ReportSettings>> optInReports)
    {
        Dictionary<string, List<APIReport<ReportSettings>>> optInReportDictionary = new();
        optInReports = new List<APIReport<ReportSettings>>();

        if (optInReportLookup.OptInReports == null)
        {
            return optInReportDictionary;
        }

        optInReports = _allReportTypes
            .Where(x => optInReportLookup.OptInReports.Any(r => string.Equals(r, x.APIReportName, StringComparison.CurrentCultureIgnoreCase))).ToList();

        _logger.LogOptInReports(optInReports.Select(x => x.APIReportName));

        if (optInReportLookup.EntityIDs == null)
            return optInReportDictionary;

        foreach (string entity in optInReportLookup.EntityIDs.Distinct())
        {
            optInReportDictionary.Add(entity, optInReports);
        }

        return optInReportDictionary;
    }

    private List<ApiReportItem> RequestReports(
        ActiveEntities activeEntities,
        APIReport<ReportSettings> report,
        IFileItem queueItem,
        TwitterRetry twitterApiCallRetry)
    {
        List<ApiReportItem> reportList = new();

        IEnumerable<ApiEntityItem> entityIdByPlacement = GetEntityIdByPlacement(activeEntities);

        foreach (ApiEntityItem placementEntityItem in entityIdByPlacement)
        {
            ProcessPlacementEntityItem(placementEntityItem, report, queueItem, twitterApiCallRetry,
                reportList);
        }

        return reportList;
    }

    private void ProcessPlacementEntityItem(
        ApiEntityItem placementEntityItem,
        APIReport<ReportSettings> reportToRequest,
        IFileItem queueItem,
        TwitterRetry twitterApiCallRetry,
        List<ApiReportItem> reportList)
    {
        IEnumerable<IEnumerable<string>> placementBatches =
            Utilities.UtilsText.GetSublistFromList(placementEntityItem.EntityIdList, EntityIdsBatchSize);

        foreach (IEnumerable<string> batch in placementBatches)
        {
            KeyValuePair<string, string>? segmentation = CreateSegmentation(reportToRequest);

            ReportRequestResponse reportRequestResponse = twitterApiCallRetry.Execute(() =>
                GetFactDataAsync(reportToRequest, queueItem, batch.ToList(), placementEntityItem, segmentation)
                    .GetAwaiter()
                    .GetResult());

            ApiReportItem reportItem = CreateReportItem(reportRequestResponse, reportToRequest, queueItem);
            reportList.Add(reportItem);
        }
    }

    private static KeyValuePair<string, string>? CreateSegmentation(APIReport<ReportSettings> report)
    {
        return !string.IsNullOrEmpty(report.ReportSettings.SegmentationType)
            ? new KeyValuePair<string, string>(report.ReportSettings.SegmentationType,
                report.ReportSettings.SegmentationValue)
            : null;
    }

    private static ApiReportItem CreateReportItem(ReportRequestResponse reportRequestResponse,
        APIReport<ReportSettings> report,
        IFileItem queueItem)
    {
        ApiReportItem reportItem = new()
        {
            QueueID = queueItem.ID,
            FileGuid = queueItem.FileGUID,
            ReportID = reportRequestResponse.Data.Id,
            ReportName = report.APIReportName,
            AccountID = queueItem.EntityID,
            StartTime = queueItem.FileDate,
            EndTime = queueItem.FileDate.AddDays(1),
            ReportURL = reportRequestResponse.Data.UrlAddress,
            Placement = reportRequestResponse.Data.Placement,
            Status = reportRequestResponse.Data.Status
        };

        if (report.ReportSettings.SegmentationType == SegmentationTypes.Country)
        {
            reportItem.DMACountryID = report.ReportSettings.SegmentationValue;
        }

        return reportItem;
    }

    private async Task<ReportRequestResponse> GetFactDataAsync(
        APIReport<ReportSettings> reportToRequest,
        IFileItem queueItem,
        List<string> entityIds,
        ApiEntityItem placementEntityItem,
        KeyValuePair<string, string>? segmentationValue = null)
    {
        GetFactReportOptions options = new()
        {
            AccountId = queueItem.EntityID,
            SegmentationType = segmentationValue?.Key,
            SegmentationValue = segmentationValue?.Value,
            Entity = reportToRequest.ReportSettings.Entity,
            FileDate = queueItem.FileDate,
            Granularity = reportToRequest.ReportSettings.Granularity,
            Placement = placementEntityItem.Placement,
            MetricGroups = reportToRequest.ReportSettings.MetricGroups,
            Segmentation = reportToRequest.ReportSettings.Segmentation,
            EntityIds = entityIds,
            ReportType = reportToRequest.ReportSettings.ReportType,
        };

        return await _apiClient.GetFactReportAsync(options);
    }

    /// <summary>
    /// active-entities returns two placements: "ALL_ON_TWITTER","PUBLISHER_NETWORK"
    /// </summary>
    /// <param name="activeEntities"></param>
    /// <returns></returns>
    private static IEnumerable<ApiEntityItem> GetEntityIdByPlacement(ActiveEntities activeEntities)
    {
        IEnumerable<string> placements = activeEntities.Data.SelectMany(i => i.Placements).Distinct();

        return placements.Select(placement => new ApiEntityItem()
        {
            Placement = placement,
            EntityIdList = activeEntities.Data.Where(d => d.Placements.Contains(placement)).Select(d => d.EntityId)
                .ToList()
        });
    }

    private void HandleExceptions()
    {
        if (_exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCounter}; Please check Splunk for more detail.");
        }
    }

    #region PreExecute Helpers

    private void SetupLogger()
    {
        _logger = new LoggerHandler(NLog.LogManager.GetCurrentClassLogger(), PrefixJobGuid, base.DefaultJobCacheKey,
            this.CurrentSource.SourceName);

        _logger.LogPreExecuteInfo();
    }

    private void InitializeApiClient()
    {
        ApiClientOptions apiClientOptions = new()
        {
            Version = CurrentCredential.CredentialSet.Version,
            EndpointUri = CurrentCredential.CredentialSet.Endpoint
        };

        _apiClient = new ApiClient(apiClientOptions, _httpClientProvider, _oAuth);
    }

    private void ConfigureBackOffStrategies()
    {
        _apiRetryBackOffStrategy = CreateBackOffStrategy(Constants.TWITTER_BACKOFF_DETAILS);
        _downloadRetryBackOffStrategy = CreateBackOffStrategy(Constants.TWITTER_RETRY_DOWNLOAD_DETAILS);
    }

    private static ExponentialBackOffStrategy CreateBackOffStrategy(string lookupKey)
    {
        string backoffValue = SetupService.GetById<Lookup>(lookupKey).Value;
        TwitterBackoff backoffDetails = JsonConvert.DeserializeObject<TwitterBackoff>(backoffValue);
        return new ExponentialBackOffStrategy { Counter = backoffDetails.Counter, MaxRetry = backoffDetails.MaxRetry };
    }

    private void LoadQueueItems()
    {
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult,
            this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)?.ToList();
    }

    private void SetupReports()
    {
        _allReportTypes = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        SetupOptInReports();
        _apiEntities = JobService.GetAllActiveAPIEntities(CurrentSource.SourceID);
    }

    private void SetupOptInReports()
    {
        ApiReportLookup apiReportLookup = GetApiReportLookup();

        // based on lookup value, the reports listed are to be opted-in by the entities listed
        // these reports should be removed from the default set of active API reports because they are subscription-based
        // if no lookup is provided, then all API reports are considered default and will be used
        _optInReportDictionary = GetOptInReports(apiReportLookup, out List<APIReport<ReportSettings>> optInReports);
        _reports = _allReportTypes.Except(optInReports);
    }

    private static ApiReportLookup GetApiReportLookup()
    {
        string lookupValue = SetupService.GetById<Lookup>(Constants.TWITTER_OPT_IN_REPORTS)?.Value;
        return string.IsNullOrEmpty(lookupValue)
            ? new ApiReportLookup()
            : ETLProvider.DeserializeType<ApiReportLookup>(lookupValue);
    }

    private void LoadUnfinishedReports()
    {
        // Getting the list of unfinished reports from previous import
        _unfinishedReportLookup =
            SetupService.GetById<Lookup>($"{Constants.TWITTER_UNFINISHED_REPORTS}_{CurrentIntegration.IntegrationID}");
        if (!string.IsNullOrEmpty(_unfinishedReportLookup?.Value))
        {
            ProcessUnfinishedReports();
        }
    }

    private void ProcessUnfinishedReports()
    {
        // getting unfinished reports from previous import jobs
        _unfinishedReports =
            JsonConvert.DeserializeObject<Dictionary<long, List<ApiReportItem>>>(_unfinishedReportLookup.Value);

        // retrieving the list of unfinished reports that wont be imported now because the queues they are associated with
        // are not in the current selection
        _reportsMissingFromQueue = _unfinishedReports
            .Where(r => _queueItems.All(q => q.ID != r.Key))
            .SelectMany(r => r.Value)
            .ToList();

        // unfinishedReports that match the list of queue to be imported
        _unfinishedReports = _unfinishedReports
            .Where(r => _queueItems.Any(q => q.ID == r.Key))
            .ToDictionary(r => r.Key, r => r.Value);

        UpdateQueueItemsWithUnfinishedReports();
    }

    private void UpdateQueueItemsWithUnfinishedReports()
    {
        // now that unfinishedReports contains the list of unfinished reports to import, we update the queues 
        // with the files already downloaded
        foreach (IFileItem queue in _queueItems)
        {
            if (_unfinishedReports.TryGetValue(queue.ID, out List<ApiReportItem> unfinishedReport))
            {
                IEnumerable<ApiReportItem> downloadedReports =
                    unfinishedReport.Where(r => r.IsReady && r.IsDownloaded);
                foreach (ApiReportItem report in downloadedReports)
                {
                    AddToQueueFileCollection(queue, report);
                }
            }
            else
            {
                ResetQueueFileCollection(queue);
            }
        }
    }

    private static void ResetQueueFileCollection(IFileItem queue)
    {
        queue.FileCollectionJSON = null;
        queue.DeliveryFileDate = null;
        queue.FileSize = 0;
    }

    #endregion

    public void PostExecute()
    {
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}

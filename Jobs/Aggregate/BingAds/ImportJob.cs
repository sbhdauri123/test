using Greenhouse.Auth;
using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.BingAds.Reporting;
using Greenhouse.Data.DataSource.BingAds;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.BingAds;

[Export("BingAds-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
    private RemoteAccessClient _remoteAccessClient;

    private Uri _baseDestUri;
    private List<APIReport<ReportSettings>> _reports;
    private int _exceptionsCounter;
    private List<IFileItem> _queueItems;
    private Reports _bingApiReport;
    private readonly Stopwatch _runtime = new();
    private TimeSpan _maxRuntime;
    private List<ReportResponse> _unfinishedReports;
    private List<ReportResponse> _reportsNotThisTime;
    private int _maxRetry;
    private string JobGUID => base.JED.JobGUID.ToString();
    private string UnfinishedReportsKey => $"{Constants.BINGADS_UNFINISHED_REPORTS}_{CurrentIntegration.IntegrationID}";

    private const int DefaultMaxRuntimeInHours = 3;
    private const int ExponentialBackOffStrategyCounter = 1;

    private IHttpClientProvider _httpClientProvider;
    private ITokenCache _tokenCache;
    private ITokenApiClient _tokenApiClient;

    public void PreExecute()
    {
        // dependencies that later on can be moved to constructor injection 
        _httpClientProvider ??= base.HttpClientProvider;
        _tokenCache ??= base.TokenCache;
        _tokenApiClient = new TokenApiClient(_httpClientProvider, _tokenCache, CurrentCredential);

        Stage = Constants.ProcessingStage.RAW;

        base.Initialize();
        LogPreExecuteInfo();

        _baseDestUri = GetDestinationFolder();
        _remoteAccessClient = base.GetS3RemoteAccessClient();
        _reports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId)?.ToList();
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.BINGADS_MAX_RUNTIME,
            TimeSpan.FromHours(DefaultMaxRuntimeInHours));
        _queueItems = GetQueueItems(nbTopResult: LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID));
        _maxRetry =
            LookupService.GetLookupValueWithDefault(Constants.BINGADS_CREATE_REPORT_MAX_RETRY, defaultValue: 3);

        _bingApiReport = new Reports(
            base.CurrentCredential,
            _tokenApiClient,
            (msg) => _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(msg)))
        );

        InitializeUnfinishedReports();
    }

    public void Execute()
    {
        if (_queueItems.Count == 0)
        {
            LogNoReportsInQueue();
            return;
        }

        _runtime.Start();

        ProcessQueueItems();

        _runtime.Stop();

        HandleExceptions();

        if (HasRuntimeExceeded())
        {
            LogRuntimeExceeded();

            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message =
                "Unfinished reports. Some reports were not ready during this Import and will be picked up by the next one.";
        }
    }

    private void ProcessQueueItems()
    {
        Dictionary<string, IEnumerable<dynamic>> reports = GetFlattenedReports();

        List<ReportResponse> apiReportList = new();

        foreach (IFileItem queueItem in _queueItems)
        {
            if (HasRuntimeExceeded())
            {
                LogRuntimeExceeded();
                break;
            }

            ProcessQueueItem(reports, queueItem, apiReportList);
        }

        if (!HasRuntimeExceeded())
        {
            CheckAndDownloadReports(apiReportList);
        }
    }

    private void ProcessQueueItem(Dictionary<string, IEnumerable<dynamic>> reports, IFileItem queueItem,
        List<ReportResponse> apiReportList)
    {
        try
        {
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            if (HasUnfinishedReports(queueItem, apiReportList))
            {
                return;
            }

            // When queue is retried, the collection needs to be reset to avoid invalid files.
            queueItem.FileCollectionJSON = string.Empty;

            string reportNameFormat = $"{{0}}_{queueItem.FileDate:yyyyMMdd}";

            ExponentialBackOffStrategy exponentialBackOffStrategy = new()
            {
                MaxRetry = _maxRetry,
                Counter = ExponentialBackOffStrategyCounter
            };

            CancellableRetry policy = new(JobGUID, exponentialBackOffStrategy, _runtime,
                _maxRuntime);

            foreach (KeyValuePair<string, IEnumerable<dynamic>> report in reports)
            {
                (string reportType, IEnumerable<dynamic> _) = report;

                string reportName = string.Format(reportNameFormat, reportType);

                LogReportRequest(queueItem, reportName);

                SubmitGenerateReportResponse requestReportResponse =
                    RequestGenerateReport(queueItem, reportName, report);

                // If there are failed reports, then no need to run any further.
                // Most likely continue to fail for all.
                if (requestReportResponse == null)
                {
                    HandleReportGenerateRequestFailure(apiReportList, queueItem);
                    break;
                }

                apiReportList.Add(new ReportResponse
                {
                    QueueID = queueItem.ID,
                    ReportType = reportType,
                    ReportName = reportName,
                    ApiResponse = requestReportResponse
                });
            }
        }
        catch (Exception ex)
        {
            HandleReportGenerateRequestFailure(apiReportList, queueItem);
            LogProcessQueueItemFailure(queueItem, ex);
        }
    }

    private SubmitGenerateReportResponse RequestGenerateReport(IFileItem queueItem,
        string reportName,
        KeyValuePair<string, IEnumerable<dynamic>> report)
    {
        ExponentialBackOffStrategy exponentialBackOffStrategy = new()
        {
            MaxRetry = _maxRetry,
            Counter = ExponentialBackOffStrategyCounter
        };

        CancellableRetry policy = new(JobGUID, exponentialBackOffStrategy, _runtime,
            _maxRuntime);

        ReportTime dateRange = CreateDateRange(queueItem.FileDate);
        (string reportType, IEnumerable<dynamic> columnNames) = report;

        switch (reportType)
        {
            case ReportTypes.AccountPerformance:
                return _bingApiReport.CreateReport<AccountPerformanceReportRequest, AccountPerformanceReportColumn>
                (queueItem.EntityID, reportName, columnNames.Cast<AccountPerformanceReportColumn>(), dateRange,
                    policy);
            case ReportTypes.AdDynamicTextPerformance:
                return _bingApiReport
                    .CreateReport<AdDynamicTextPerformanceReportRequest, AdDynamicTextPerformanceReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<AdDynamicTextPerformanceReportColumn>(),
                        dateRange, policy);
            case ReportTypes.AdGroupPerformance:
                return _bingApiReport.CreateReport<AdGroupPerformanceReportRequest, AdGroupPerformanceReportColumn>
                (queueItem.EntityID, reportName, columnNames.Cast<AdGroupPerformanceReportColumn>(), dateRange,
                    policy);
            case ReportTypes.AdPerformance:
                return _bingApiReport.CreateReport<AdPerformanceReportRequest, AdPerformanceReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<AdPerformanceReportColumn>(), dateRange, policy);
            case ReportTypes.BudgetSummary:
                return _bingApiReport.CreateReport<BudgetSummaryReportRequest, BudgetSummaryReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<BudgetSummaryReportColumn>(), dateRange, policy);
            case ReportTypes.CampaignPerformance:
                return _bingApiReport.CreateReport<CampaignPerformanceReportRequest, CampaignPerformanceReportColumn>
                (queueItem.EntityID, reportName, columnNames.Cast<CampaignPerformanceReportColumn>(), dateRange,
                    policy);
            case ReportTypes.ConversionPerformance:
                return _bingApiReport
                    .CreateReport<ConversionPerformanceReportRequest, ConversionPerformanceReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<ConversionPerformanceReportColumn>(),
                        dateRange, policy);
            case ReportTypes.DestinationUrlPerformance:
                return _bingApiReport
                    .CreateReport<DestinationUrlPerformanceReportRequest, DestinationUrlPerformanceReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<DestinationUrlPerformanceReportColumn>(),
                        dateRange, policy);
            case ReportTypes.KeywordPerformance:
                return _bingApiReport.CreateReport<KeywordPerformanceReportRequest, KeywordPerformanceReportColumn>
                (queueItem.EntityID, reportName, columnNames.Cast<KeywordPerformanceReportColumn>(), dateRange,
                    policy);
            case ReportTypes.PublisherUsagePerformance:
                return _bingApiReport
                    .CreateReport<PublisherUsagePerformanceReportRequest, PublisherUsagePerformanceReportColumn>
                    (queueItem.EntityID, reportName, columnNames.Cast<PublisherUsagePerformanceReportColumn>(),
                        dateRange, policy);
            default:
                throw new InvalidOperationException($"Could not create report for report type '{reportType}'");
        }
    }

    private static ReportTime CreateDateRange(DateTime fileDate)
    {
        return new ReportTime
        {
            ReportTimeZone = ReportTimeZone.GreenwichMeanTimeDublinEdinburghLisbonLondon,
            CustomDateRangeEnd = new Date { Year = fileDate.Year, Day = fileDate.Day, Month = fileDate.Month },
            CustomDateRangeStart = new Date { Year = fileDate.Year, Day = fileDate.Day, Month = fileDate.Month }
        };
    }

    private bool HasRuntimeExceeded() => _runtime.Elapsed > _maxRuntime;

    private bool HasUnfinishedReports(IFileItem queue, List<ReportResponse> apiReportList)
    {
        List<ReportResponse> unfinishedReports = _unfinishedReports.Where(u => u.QueueID == queue.ID).ToList();
        if (unfinishedReports.Count == 0)
        {
            return false;
        }

        apiReportList.AddRange(unfinishedReports);
        return true;
    }

    private void CheckAndDownloadReports(List<ReportResponse> reportList)
    {
        if (reportList.Count == 0)
        {
            LogNoReportsToDownload();
            return;
        }

        ExponentialBackOffStrategy apiCallsBackOffStrategy = new()
        {
            Counter = ExponentialBackOffStrategyCounter,
            MaxRetry = _maxRetry
        };

        CancellableConditionalRetry<bool> policy = new(
            JobGUID,
            apiCallsBackOffStrategy,
            _runtime,
            _maxRuntime,
            allReportsDone => !allReportsDone
        );

        policy.Execute(() =>
        {
            List<ReportResponse> pendingReports = reportList.Where(x => !x.IsReady).ToList();

            foreach (ReportResponse currReport in pendingReports)
            {
                if (HasRuntimeExceeded())
                {
                    LogCheckAndDownloadReportsMaximumRuntimeExceeded();
                    return true;
                }

                ProcessReport(currReport, reportList);
            }

            return reportList.All(x => x.IsReady);
        });

        _runtime.Stop();
        SaveUnfinishedReports(reportList);
    }

    private void ProcessReport(ReportResponse currReport, List<ReportResponse> reportList)
    {
        IFileItem queueItem = _queueItems.FirstOrDefault(x => x.ID == currReport.QueueID);

        if (currReport.IsReady)
        {
            return;
        }

        try
        {
            PollGenerateReportResponse reportStatus =
                _bingApiReport.GetReportStatus(currReport.ApiResponse?.ReportRequestId);

            LogReportStatus(currReport, reportStatus);

            switch (reportStatus?.ReportRequestStatus.Status)
            {
                case ReportRequestStatusType.Success:
                    HandleSuccessfulReport(currReport, reportStatus, queueItem, reportList);
                    break;
                case ReportRequestStatusType.Error:
                    HandleFailedReport(currReport, queueItem);
                    break;
            }
        }
        catch (HttpClientProviderRequestException exc)
        {
            HandleReportException(currReport, queueItem, exc);
        }
        catch (Exception exc)
        {
            HandleReportException(currReport, queueItem, exc);
        }
    }

    private void HandleSuccessfulReport(ReportResponse currReport, PollGenerateReportResponse reportStatus,
        IFileItem queueItem, List<ReportResponse> reportList)
    {
        string downloadUrl = reportStatus.ReportRequestStatus?.ReportDownloadUrl;
        currReport.IsReady = true;
        currReport.IsDownloaded = DownloadReport(currReport, downloadUrl, queueItem);

        if (currReport.IsDownloaded)
        {
            if (IsQueueCompleted(reportList, currReport.QueueID))
            {
                CompleteQueueItem(currReport, queueItem);
            }
        }
        else
        {
            LogDownloadFailure(currReport, queueItem);
            _exceptionsCounter++;
            ProcessDownloadReportError(queueItem);
        }
    }


    private void HandleFailedReport(ReportResponse currReport, IFileItem queueItem)
    {
        LogReportError(currReport, queueItem);
        _exceptionsCounter++;
        ProcessDownloadReportError(queueItem);
    }

    private void HandleReportException(ReportResponse currReport, IFileItem queueItem, Exception exc)
    {
        _exceptionsCounter++;
        ProcessDownloadReportError(queueItem);
        LogReportException(currReport, queueItem, exc);
    }

    private void HandleReportException(ReportResponse currReport, IFileItem queueItem, HttpClientProviderRequestException exc)
    {
        _exceptionsCounter++;
        ProcessDownloadReportError(queueItem);
        LogReportException(currReport, queueItem, exc);
    }

    private static bool IsQueueCompleted(List<ReportResponse> reportList, long queueId)
        => reportList.Where(x => x.QueueID == queueId).All(x => x.IsDownloaded && x.IsReady);

    private static void CompleteQueueItem(ReportResponse currReport, IFileItem queueItem)
    {
        currReport.Status = ReportRequestStatusType.Success;
        currReport.IsReady = true;
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update<Queue>((Queue)queueItem);
    }

    private static void ProcessDownloadReportError(IFileItem queueItem)
    {
        queueItem.FileCollectionJSON = null;
        queueItem.FileSize = 0;
        queueItem.Status = Constants.JobStatus.Error.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        JobService.Update((Queue)queueItem);
    }

    private void HandleReportGenerateRequestFailure(List<ReportResponse> apiReportList, IFileItem queueItem)
    {
        _exceptionsCounter++;
        LogGenerateReportRequestFailure(queueItem);

        apiReportList.RemoveAll(x => x.QueueID == queueItem.ID);

        queueItem.Status = Constants.JobStatus.Error.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
    }


    private void SaveUnfinishedReports(List<ReportResponse> reportList)
    {
        // any queue that has report still pending has status set to pending
        // if any of its report was ready it was downloaded and added to its FileCollectionItems
        // we are going to save any pending report in Lookup

        List<IFileItem> unfinishedQueues = _queueItems.Where(q => reportList.Any(r => !r.IsReady)).ToList();

        if (unfinishedQueues.Count != 0)
        {
            LogUnfinishedQueues(unfinishedQueues);

            unfinishedQueues.ToList().ForEach(q =>
            {
                q.Status = Constants.JobStatus.Pending.ToString();
                q.StatusId = (int)Constants.JobStatus.Pending;
                // this will also save to the DB the FileCollectionJSON containing the reports downloaded
                JobService.Update((Queue)q);
            });
        }

        // getting the list of all QueueIDs for the current integration
        // to clean the list of reports not imported this time from any reports related to a 
        // queue that does not exist anymore
        List<long> queueIdList =
            JobService.GetQueueIDBySource(base.SourceId, CurrentIntegration.IntegrationID)?.ToList() ?? new List<long>();

        IEnumerable<ReportResponse> unfinishedReports = reportList.Where(x => !x.IsReady)
            .Concat(_reportsNotThisTime.Where(r => queueIdList.Contains(r.QueueID)));

        Lookup lookup = new()
        {
            Name = UnfinishedReportsKey,
            Value = JsonConvert.SerializeObject(unfinishedReports),
            LastUpdated = DateTime.Now
        };

        Data.Repositories.LookupRepository.AddOrUpdateLookup(lookup);
        LogUnfinishedReportsStoredInLookup(unfinishedReports);
    }

    private (string fileExtension, string fileFormat) GetFileDetails(ReportResponse reportItem)
    {
        APIReport<ReportSettings> report = _reports.FirstOrDefault(f => f.APIReportName == reportItem.ReportType);
        string fileExtension = report?.ReportSettings?.FileExtension ?? "zip";
        string fileFormat = report?.ReportSettings?.FileFormat ?? "csv";
        return (fileExtension, fileFormat);
    }

    private (string[] paths, S3File rawFile) CreateS3File(IFileItem queueItem, string fileName)
    {
        string[] paths = { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName };
        return (paths, new S3File(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds));
    }

    private static void HandleEmptyUrl(S3File rawFile, string fileExtension, string fileFormat,
        ReportResponse reportItem)
    {
        if (fileExtension == "zip")
        {
            CreateEmptyZipFile(rawFile, fileFormat, reportItem);
        }
        else
        {
            CreateEmptyFile(rawFile);
        }
    }

    private static void CreateEmptyZipFile(S3File rawFile, string fileFormat, ReportResponse reportItem)
    {
        using MemoryStream memoryStream = new();
        using (ZipArchive archive = new(memoryStream, ZipArchiveMode.Create, true))
        {
            archive.CreateEntry($"{reportItem.ApiResponse.ReportRequestId}.{fileFormat}");
        }

        memoryStream.Position = 0;
        rawFile.Put(memoryStream);
    }

    private static void CreateEmptyFile(S3File rawFile)
    {
        using Stream rawFileStream = rawFile.Create();
    }

    private async Task DownloadAndSaveFileAsync(string url, S3File rawFile, string[] paths)
    {
        await using Stream responseStream = await _httpClientProvider.DownloadFileStreamAsync(
            new HttpRequestOptions { Uri = url, Method = HttpMethod.Get, Headers = [] });

        StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);
    }

    private static void UpdateQueueItem(IFileItem queueItem, ReportResponse reportItem, S3File rawFile, string fileName)
    {
        List<FileCollectionItem> files = queueItem.FileCollection?.ToList() ?? new List<FileCollectionItem>();
        FileCollectionItem fileItem = new()
        {
            FileSize = rawFile.Length,
            SourceFileName = reportItem.ReportName,
            FilePath = fileName
        };
        files.Add(fileItem);

        queueItem.FileCollectionJSON = JsonConvert.SerializeObject(files);
        queueItem.FileSize += rawFile.Length;
    }

    private bool DownloadReport(ReportResponse reportItem, string url, IFileItem queueItem1)
    {
        bool returnVal;
        IFileItem queueItem = _queueItems.FirstOrDefault(q => q.ID == reportItem.QueueID);
        try
        {
            (string fileExtension, string fileFormat) = GetFileDetails(reportItem);
            string fileName = $"{reportItem.ReportName}_{reportItem.ApiResponse.ReportRequestId}.{fileExtension}".ToLower();

            LogDownloadReportStarted(reportItem, url, queueItem, fileName);

            (string[] paths, S3File rawFile) = CreateS3File(queueItem, fileName);

            if (string.IsNullOrEmpty(url))
            {
                HandleEmptyUrl(rawFile, fileExtension, fileFormat, reportItem);
            }
            else
            {
                DownloadAndSaveFileAsync(url, rawFile, paths).GetAwaiter().GetResult();
            }

            UpdateQueueItem(queueItem, reportItem, rawFile, fileName);

            returnVal = true;
            LogReportDownloaded(reportItem, queueItem, fileName);
        }
        catch (HttpClientProviderRequestException exc)
        {
            returnVal = false;
            HandleReportDownloadException(reportItem, queueItem, exc);
        }
        catch (Exception exc)
        {
            returnVal = false;
            HandleReportDownloadException(reportItem, queueItem, exc);
        }

        return returnVal;
    }
    private void HandleReportDownloadException(ReportResponse reportItem, IFileItem queueItem, HttpClientProviderRequestException exc)
    {
        _exceptionsCounter++;
        LogReportDownloadError(reportItem, queueItem, exc);
    }
    private void HandleReportDownloadException(ReportResponse reportItem, IFileItem queueItem, Exception exc)
    {
        _exceptionsCounter++;
        LogReportDownloadError(reportItem, queueItem, exc);
    }


    private static Assembly GetBingAdsAssembly()
    {
        return AppDomain.CurrentDomain.GetAssemblies()
                   .FirstOrDefault(assembly =>
                       assembly.FullName?.StartsWith("Greenhouse.DAL", StringComparison.OrdinalIgnoreCase) == true)
               ?? throw new InvalidOperationException("BingAds assembly not found");
    }

    private Dictionary<string, IEnumerable<dynamic>> GetFlattenedReports()
    {
        if (_reports == null || _reports.Count == 0)
        {
            return new Dictionary<string, IEnumerable<dynamic>>();
        }

        Dictionary<string, IEnumerable<dynamic>> flattenedReports = new();

        StringBuilder validationMessages = new();
        StringBuilder validationErrors = new();
        validationMessages.AppendLine(
            PrefixJobGuid("Start validating ReportFields in Configuration to BingAds report setting"));

        bool validationStatus = true;

        foreach (APIReport<ReportSettings> report in _reports)
        {
            validationStatus &= ValidateReport(report, flattenedReports, validationMessages, validationErrors);
        }

        validationMessages.AppendLine(
            PrefixJobGuid("End validating ReportFields in Configuration to BingAds report setting"));

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid($"BingAds ValidateReportFields complete. {validationMessages}; Errors: {validationErrors}")));

        if (!validationStatus)
        {
            throw new InvalidOperationException(
                PrefixJobGuid("Error: not all report fields are valid. Please fix report field(s)."));
        }

        return flattenedReports;
    }

    private bool ValidateReport(APIReport<ReportSettings> report,
        Dictionary<string, IEnumerable<dynamic>> lists, StringBuilder validationMessages,
        StringBuilder validationErrors)
    {
        Assembly assembly = GetBingAdsAssembly();
        const string nsFormat = "Greenhouse.DAL.BingAds.Reporting.{0}";
        try
        {
            Type dataType = assembly.GetType(string.Format(nsFormat, report.ReportSettings.ReportType), false, true);
            object instance = Activator.CreateInstance(dataType);
            validationMessages.AppendLine(PrefixJobGuid(
                $"Report Name: {report.APIReportName}; Report Type Columns: {report.ReportSettings.ReportType}"));

            List<string> rawColumns = report.ReportFields.Select(x => x.APIReportFieldName).ToList();
            List<object> matchedColumns = UtilsText.ConvertToEnum(rawColumns, instance).ToList();
            List<string> unmatchedColumns = matchedColumns.Select(x => x.ToString())
                .Except(rawColumns, StringComparer.OrdinalIgnoreCase).ToList();

            if (unmatchedColumns.Count != 0)
            {
                validationErrors.AppendLine(
                    PrefixJobGuid($"{report.APIReportName} unmatched {string.Join(",", unmatchedColumns)}"));
                return false;
            }

            validationMessages.AppendLine(PrefixJobGuid($"{report.APIReportName} all columns matched"));
            lists.Add(report.APIReportName, matchedColumns);

            return true;
        }
        catch (Exception ex)
        {
            validationErrors.AppendLine(PrefixJobGuid(
                $"Exception: trying to compare columns for Report Name: {report.APIReportName}; " +
                $"Report Type Columns: {report.ReportSettings.ReportType}; Exception: {ex.Message};"));
            return false;
        }
    }

    public void PostExecute()
    {
    }

    #region PreExecute Helpers

    /// <summary>
    /// initialize a new list of report items reserved for any cached report items not in this job's queue item list
    /// </summary>
    private void InitializeUnfinishedReports()
    {
        _unfinishedReports = new List<ReportResponse>();
        _reportsNotThisTime = new List<ReportResponse>();

        Lookup unfinishedReportsLookup = JobService.GetById<Lookup>(UnfinishedReportsKey);
        if (_queueItems == null || string.IsNullOrEmpty(unfinishedReportsLookup?.Value))
        {
            return;
        }

        List<ReportResponse> allUnfinishedReports =
            JsonConvert.DeserializeObject<List<ReportResponse>>(unfinishedReportsLookup.Value);

        // only select the queue imported this time
        _unfinishedReports = allUnfinishedReports
            .Where(r => _queueItems.Any(q => q.ID == r.QueueID)).ToList();

        // keep track of the reports not imported this time    
        _reportsNotThisTime = allUnfinishedReports
            .Where(r => _queueItems.All(q => q.ID != r.QueueID)).ToList();
    }

    private List<IFileItem> GetQueueItems(int nbTopResult)
        => JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult,
            this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)?.ToList();

    #endregion

    #region Exceptions

    private void HandleExceptions()
    {
        if (_exceptionsCounter > 0)
        {
            throw new ErrorsFoundException(
                PrefixJobGuid($"Total errors: {_exceptionsCounter}; Please check Splunk for more detail."));
        }
    }

    #endregion

    #region Logs

    private void LogRuntimeExceeded()
    {
        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
            base.PrefixJobGuid(
                $"Runtime exceeded time allotted - {_runtime.ElapsedMilliseconds}ms")));
    }

    private void LogCheckAndDownloadReportsMaximumRuntimeExceeded()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid(
                $"The job max runtime ({_maxRuntime}) was reached: {_runtime.Elapsed} in CheckAndDownloadReports")));


    private void LogNoReportsInQueue()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"BingAds no queue items to process")));

    private void LogProcessQueueItemFailure(IFileItem queue, Exception exception)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid(
                $"Error - {exception.Message} Removing all entries related to QueueID: {queue.ID}"), exception));
    }

    private void LogReportRequest(IFileItem queue, string reportName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid($"Creating report {reportName} for queueID: {queue.ID}")));
    }

    private void LogPreExecuteInfo()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));


    private void LogNoReportsToDownload()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"There are no reports to download.")));

    private void LogUnfinishedReportsStoredInLookup(IEnumerable<ReportResponse> unfinishedReports)
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid(
                $"Stored unfinished reports in Lookup {UnfinishedReportsKey} - count: {unfinishedReports.Count()}")));

    private void LogUnfinishedQueues(List<IFileItem> unfinishedQueues)
        => _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
            PrefixJobGuid($"Unfinished Reports QueueIDs: {string.Join(",", unfinishedQueues.Select(fq => fq.ID))}")));


    private void LogReportStatus(ReportResponse currReport, PollGenerateReportResponse reportStatus)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid(
                $"Report Status: {reportStatus?.ReportRequestStatus.Status}->QueueID: {currReport.QueueID}->Report Name: {currReport.ReportType}")));
    }

    private void LogDownloadFailure(ReportResponse currReport, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Error: The report could not be downloaded - failed on queueID: {currReport.QueueID}->" +
                          $"for EntityID: {queueItem.EntityID}->Report ID: {currReport.ApiResponse.ReportRequestId}->Report Type:{currReport.ReportType}")));
    }

    private void LogReportError(ReportResponse currReport, IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Error ReportRequestStatus.Status = Error - failed on queueID: {currReport.QueueID}->" +
                          $"for EntityID: {queueItem.EntityID}->Report ID: {currReport.ApiResponse.ReportRequestId}->Report Type:{currReport.ReportType}")));
    }

    private void LogReportException(ReportResponse currReport, IFileItem queueItem, Exception exc)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Error checking report status - failed on queueID: {currReport.QueueID}->" +
                          $"for EntityID: {queueItem.EntityID}->Report ID: {currReport.ApiResponse.ReportRequestId}->Report Type:{currReport.ReportType}->" +
                          $"Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
    }

    private void LogReportException(ReportResponse currReport, IFileItem queueItem, HttpClientProviderRequestException exc)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Error checking report status - failed on queueID: {currReport.QueueID}->" +
                          $"for EntityID: {queueItem.EntityID}->Report ID: {currReport.ApiResponse.ReportRequestId}->Report Type:{currReport.ReportType}->" +
                          $"Exception details: {exc}"), exc));
    }

    private void LogReportDownloadError(ReportResponse reportItem, IFileItem queueItem, Exception exc)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            base.PrefixJobGuid(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {queueItem.EntityID} " +
                $"->Report ID: {reportItem.ApiResponse.ReportRequestId} ->Report Name: {reportItem.ReportName}" +
                $"  - Exception: {exc.Message} - STACK {exc.StackTrace}")
            , exc));
    }
    private void LogReportDownloadError(ReportResponse reportItem, IFileItem queueItem, HttpClientProviderRequestException exc)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            base.PrefixJobGuid(
                $"Error downloading report - failed on queueID: {reportItem.QueueID} for EntityID: {queueItem.EntityID} " +
                $"->Report ID: {reportItem.ApiResponse.ReportRequestId} ->Report Name: {reportItem.ReportName}" +
                $" |Exception details : {exc}")
            , exc));
    }
    private void LogReportDownloaded(ReportResponse reportItem, IFileItem queueItem, string fileName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid(
                $"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueItem.FileGUID}->queueID: {reportItem.QueueID}->Report ID: {reportItem.ApiResponse.ReportRequestId}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));
    }

    private void LogDownloadReportStarted(ReportResponse reportItem, string url, IFileItem queueItem, string fileName)
    {
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
            PrefixJobGuid(
                $"{CurrentSource.SourceName} start DownloadReport:FileGUID: {queueItem.FileGUID}->queueID: {reportItem.QueueID}->{reportItem.ApiResponse.ReportRequestId}->{reportItem.ReportName}->{url}. Saving to S3 as {fileName}")));
    }

    private void LogGenerateReportRequestFailure(IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid(
                $"BingAds Create Report failed. Removing all entries related to QueueID: {queueItem.ID}")));
    }

    #endregion

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (disposing)
        {
            _remoteAccessClient?.Dispose();
        }
    }

    ~ImportJob()
    {
        Dispose(false);
    }
}

using Greenhouse.Auth;
using Greenhouse.Caching;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.LinkedIn;
using Greenhouse.Data.DataSource.LinkedIn;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.LinkedIn;
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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.LinkedIn;

[Export("LinkedIn-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private Uri _baseDestUri;
    private readonly Stopwatch _runtime = new();

    private List<IFileItem> _queueItems;
    private TimeSpan _maxRuntime;
    private int _exceptionCount;
    private int _maxRetry;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private List<ApiReportItem> _reportList = new();
    private Dictionary<string, DateTime?> _apiEntitiesDownloadedDimensionReports;
    private Dictionary<string, List<FileCollectionItem>> _dimensionManifestFiles;
    private readonly CompositeFormat _downloaded_dim_reports = CompositeFormat.Parse(Constants.LINKEDIN_DOWNLOADED_DIM_REPORTS);
    private readonly CompositeFormat _downloaded_dim_manifest_files = CompositeFormat.Parse(Constants.LINKEDIN_DOWNLOADED_DIM_MANIFEST_FILES);

    private const int ExponentialBackOffStrategyCounter = 3;
    private const int DefaultPageSize = 100;

    private ApiClient _apiClient;
    private IHttpClientProvider _httpClientProvider;
    private ITokenCache _tokenCache;

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        _tokenCache ??= base.TokenCache;

        base.Initialize();
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.LINKEDIN_POLLY_MAX_RETRY, 10);
        Stage = Constants.ProcessingStage.RAW;
        _baseDestUri = GetDestinationFolder();

        LogPreExecuteInfo();

        _queueItems = GetQueueItems(nbTopResult: LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID));
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(SourceId);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.LINKEDIN_MAX_RUNTIME, TimeSpan.FromHours(3));
        _apiEntitiesDownloadedDimensionReports = GetAllReportsToDownload();
        _dimensionManifestFiles = GetAllDimensionManifestFiles();
        _apiClient = CreateApiClient();
    }

    public void Execute()
    {
        _runtime.Start();

        if (_queueItems.Count == 0)
        {
            LogNoReportsInQueue();
        }

        foreach (IFileItem queueItem in _queueItems)
        {
            if (HasRuntimeExceededAllottedTime())
            {
                LogRuntimeExceeded();
                break;
            }

            ProcessQueueItem(queueItem);
        }

        HandleExceptions();
        LogJobComplete();
    }

    #region Execute Helpers

    private void ProcessQueueItem(IFileItem queueItem)
    {
        _reportList = new List<ApiReportItem>();
        try
        {
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

            Reports reports = GetReportsToDownload();
            DownloadReports(queueItem, reports);

            ProcessReportList(queueItem);
        }
        catch (HttpClientProviderRequestException hex)
        {
            HandleHttpRequestException(queueItem, hex);
        }
        catch (Exception ex)
        {
            HandleGeneralException(queueItem, ex);
        }
    }

    private void DownloadReports(IFileItem queueItem, Reports reports)
    {
        DownloadFactReports(reports.FactReports, queueItem);

        if (HasDimensionDataBeenDownloadedToday(queueItem))
        {
            return;
        }

        DownloadAdAccountsReport(reports.AdAccountsReport, queueItem);
        DownloadDimensionReports(queueItem, reports);
    }

    private void ProcessReportList(IFileItem queueItem)
    {
        //If there's no report  update to processing complete otherwise create manifest files
        //Example: We don't have any fact reports and dim reports were already downloaded
        if (_reportList.Count == 0)
        {
            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
        }
        else
        {
            CreateManifestFiles(_reportList, (Queue)queueItem);
        }
    }

    private bool HasRuntimeExceededAllottedTime() => _runtime.Elapsed > _maxRuntime;

    private Reports GetReportsToDownload()
    {
        Reports reports = new();

        foreach (APIReport<ReportSettings> apiReport in _apiReports)
        {
            if (apiReport.ReportSettings.ReportType == ReportTypes.Fact)
            {
                reports.FactReports.Add(apiReport);
            }
            else if (IsAdCampaignGroupsReport(apiReport))
            {
                reports.CampaignGroupsReport ??= apiReport;
            }
            else if (IsAdCampaignsReport(apiReport))
            {
                reports.CampaignReport ??= apiReport;
            }
            else if (IsCreativesReport(apiReport))
            {
                reports.CreativesReport ??= apiReport;
            }
            else if (IsAdAccountsReport(apiReport))
            {
                reports.AdAccountsReport ??= apiReport;
            }
        }

        return reports;
    }

    private void DownloadFactReports(List<APIReport<ReportSettings>> factReports, IFileItem queueItem)
    {
        if (factReports.Count == 0)
        {
            LogNoFactReportsFound(queueItem);
            return;
        }

        try
        {
            foreach (var report in factReports)
            {
                DownloadFactReport(report, queueItem);
            }
        }
        catch (HttpClientProviderRequestException ex)
        {
            LogFactReportDownloadError(queueItem, ex);
            throw;
        }
        catch (Exception ex)
        {
            LogFactReportDownloadError(queueItem, ex);
            throw;
        }
    }

    private void DownloadFactReport(APIReport<ReportSettings> factReport, IFileItem queueItem)
    {
        ExponentialBackOffStrategy exponentialBackOffStrategy = new()
        {
            Counter = ExponentialBackOffStrategyCounter,
            MaxRetry = _maxRetry
        };

        CancellableRetry cancellableRetry = new(queueItem.FileGUID.ToString(), exponentialBackOffStrategy, _runtime, _maxRuntime);

        cancellableRetry.Execute(() =>
        {
            FactReportDownloadOptions options = CreateFactReportDownloadOptions(factReport, queueItem);
            using Stream responseStream = _apiClient.DownloadFactReportStreamAsync(options).GetAwaiter().GetResult();

            bool hasElements = CheckForElementsAsync(responseStream).GetAwaiter().GetResult();
            if (!hasElements)
            {
                return;
            }

            string fileName = BuildFactReportFileName(factReport, queueItem);
            string[] paths = BuildFilePaths(queueItem, fileName);
            S3File rawFile = UploadReportToS3(responseStream, paths);
            _reportList.Add(BuildApiReportItem(queueItem, factReport, rawFile, fileName));
        });
    }

    /// <summary>
    /// Searches for the JSON 'elements' property and checks if it's an empty array without reading the whole stream.
    /// </summary>
    /// <param name="stream"></param>
    /// <returns></returns>
    private static async Task<bool> CheckForElementsAsync(Stream stream)
    {
        using StreamReader streamReader = new(stream, leaveOpen: true);
        await using JsonTextReader jsonReader = new(streamReader);

        while (await jsonReader.ReadAsync())
        {
            if (jsonReader.Path.Equals("elements", StringComparison.CurrentCultureIgnoreCase) &&
                jsonReader.TokenType == JsonToken.StartArray)
            {
                return await jsonReader.ReadAsync() && jsonReader.TokenType != JsonToken.EndArray;
            }
        }

        return false;
    }

    private static string BuildFactReportFileName(APIReport<ReportSettings> report, IFileItem queue)
        => $"{queue.FileGUID}_{report.APIReportName}.{report.ReportSettings.FileExtension}".ToLower();

    private S3File UploadReportToS3(Stream responseStream, string[] paths)
    {
        responseStream.Seek(0, SeekOrigin.Begin);
        S3File rawFile = new(RemoteUri.CombineUri(this._baseDestUri, paths), GreenhouseS3Creds);
        StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
        UploadToS3(incomingFile, rawFile, paths);
        return rawFile;
    }

    private static FactReportDownloadOptions CreateFactReportDownloadOptions(APIReport<ReportSettings> report, IFileItem queueItem) => new()
    {
        AccountId = queueItem.EntityID,
        DeliveryPath = report.ReportSettings.DeliveryPath,
        FileDate = queueItem.FileDate,
        ReportFieldNames = report.ReportFields.Select(field => field.APIReportFieldName)
    };

    private bool HasDimensionDataBeenDownloadedToday(IFileItem queueItem)
    {
        _apiEntitiesDownloadedDimensionReports.TryGetValue(queueItem.EntityID, out DateTime? lastDownloadedDate);
        bool hasDimensionDataBeenDownloadedToday = lastDownloadedDate?.Date == DateTime.UtcNow.Date;

        if (hasDimensionDataBeenDownloadedToday)
        {
            LogDimensionDataAlreadyDownloaded(queueItem);
        }

        return hasDimensionDataBeenDownloadedToday;
    }

    private void DownloadAdAccountsReport(APIReport<ReportSettings> report, IFileItem queueItem)
    {
        if (report == null)
        {
            throw new APIReportException("Failed to retrieve AdAccounts Report.");
        }

        ExponentialBackOffStrategy exponentialBackOffStrategy = new()
        {
            Counter = ExponentialBackOffStrategyCounter,
            MaxRetry = _maxRetry
        };

        CancellableRetry cancellableRetry = new(queueItem.FileGUID.ToString(), exponentialBackOffStrategy, _runtime, _maxRuntime);

        cancellableRetry.Execute(() =>
        {
            string fileName = BuildAdAccountsReportFileName(report, queueItem);
            string[] paths = BuildFilePaths(queueItem, fileName);
            var localFile = BuildLocalFile(paths);

            DownloadAndRewriteAdAccountsReportAsync(queueItem, localFile).GetAwaiter().GetResult();

            S3File file = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
            UploadToS3(localFile, file, paths);

            localFile.Directory.Delete(true);
            _reportList.Add(BuildApiReportItem(queueItem, report, file, fileName));

            return true;
        });
    }

    private async Task DownloadAndRewriteAdAccountsReportAsync(IFileItem queueItem, FileSystemFile localFile)
    {
        AdAccountsReportDownloadOptions options = new() { AccountId = queueItem.EntityID };

        await using Stream responseStream = await _apiClient.DownloadAdAccountsReportStreamAsync(options);
        using StreamReader reader = new(responseStream, new UTF8Encoding(false));
        await using StreamWriter writer = new(localFile.FullName, true, new UTF8Encoding(false));

        string responseString = await reader.ReadToEndAsync();
        await writer.WriteAsync("{\"data\" :");
        await writer.WriteAsync(responseString.ToCharArray());
        await writer.WriteAsync('}');
    }

    private void DownloadDimensionReports(IFileItem queueItem, Reports reports)
    {
        IEnumerable<string> campaignGroupIds = DownloadDimensionReport(
            queueItem,
            reports.CampaignGroupsReport,
            _apiClient.DownloadAdCampaignGroupsReportStreamAsync);

        IEnumerable<string> campaignIds = DownloadDimensionReport(
            queueItem,
            reports.CampaignReport,
            _apiClient.DownloadAdCampaignsReportStreamAsync,
            campaignGroupIds);

        DownloadDimensionReport(
            queueItem,
            reports.CreativesReport,
            _apiClient.DownloadCreativesReportStreamAsync,
            campaignIds);

        _apiEntitiesDownloadedDimensionReports[queueItem.EntityID] = DateTime.UtcNow;
    }

    private List<string> DownloadDimensionReport(
        IFileItem queueItem,
        APIReport<ReportSettings> report,
        Func<DimensionReportDownloadOptions, Task<Stream>> downloadDimensionReport,
        IEnumerable<string> searchIds = null)
    {
        if (report == null)
        {
            return new List<string>();
        }

        int pageNumber = 0;
        string nextPageToken = string.Empty;
        List<string> entityIds = new();

        do
        {
            ExponentialBackOffStrategy exponentialBackOffStrategy = new()
            {
                Counter = ExponentialBackOffStrategyCounter,
                MaxRetry = _maxRetry
            };

            CancellableRetry cancellableRetry = new(queueItem.FileGUID.ToString(), exponentialBackOffStrategy, _runtime, _maxRuntime);
            cancellableRetry.Execute(() =>
            {
                DimensionReportDownloadOptions options = CreateDimensionReportDownloadOptions(queueItem, report, searchIds, nextPageToken);
                using Stream responseStream = downloadDimensionReport(options).GetAwaiter().GetResult();

                using StreamReader reader = new(responseStream);
                string responseString = reader.ReadToEndAsync().GetAwaiter().GetResult();
                ApiResponse apiResponse = JsonConvert.DeserializeObject<ApiResponse>(responseString);

                pageNumber++;
                nextPageToken = apiResponse.Metadata.NextPageToken;

                entityIds.AddRange(apiResponse.Elements.Select(x => x.Id).ToList());

                string fileName = BuildDimensionReportFileName(queueItem, report, pageNumber);
                string[] paths = BuildFilePaths(queueItem, fileName);

                responseStream.Seek(0, SeekOrigin.Begin);

                S3File rawFile = new(RemoteUri.CombineUri(_baseDestUri, paths), GreenhouseS3Creds);
                StreamFile incomingFile = new(responseStream, GreenhouseS3Creds);
                UploadToS3(incomingFile, rawFile, paths);

                _reportList.Add(BuildApiReportItem(queueItem, report, rawFile, fileName));
            });

        } while (!string.IsNullOrEmpty(nextPageToken));

        return entityIds;
    }

    private static DimensionReportDownloadOptions CreateDimensionReportDownloadOptions(
        IFileItem queueItem, APIReport<ReportSettings> report, IEnumerable<string> searchIds, string pageToken)
    {
        return new DimensionReportDownloadOptions
        {
            AccountId = queueItem.EntityID,
            DeliveryPath = report.ReportSettings.DeliveryPath,
            SearchIds = searchIds,
            NextPageToken = pageToken
        };
    }

    private static string BuildDimensionReportFileName(IFileItem queueItem, APIReport<ReportSettings> apireport, int numOfCalls)
        => $"{queueItem.FileGUID}_{apireport.APIReportName}_{numOfCalls}.{apireport.ReportSettings.FileExtension}".ToLower();

    private ApiReportItem BuildApiReportItem(IFileItem queueInfo, APIReport<ReportSettings> report, S3File fileInfo, string fileName)
    {
        ApiReportItem apiReportItem = new()
        {
            ReportName = report.APIReportName.ToLower(),
            QueueID = queueInfo.ID,
            FileGuid = queueInfo.FileGUID,
            ReportType = report.ReportSettings.ReportType,
            APIReport = report,
            FileDate = queueInfo.FileDate,
            AccountID = queueInfo.EntityID,
            FileName = fileName,
            FileSize = fileInfo.Length
        };

        apiReportItem.FileCollection = new FileCollectionItem()
        {
            SourceFileName = report.APIReportName,
            FilePath = BuildFilePath(queueInfo, apiReportItem),
            FileSize = fileInfo.Length
        };

        return apiReportItem;
    }

    private FileSystemFile BuildLocalFile(string[] paths)
    {
        Uri tempDestUri = RemoteUri.CombineUri(base.GetLocalImportDestinationFolder(), paths);
        var localFile = new FileSystemFile(tempDestUri);
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
        }

        return localFile;
    }

    private static string[] BuildFilePaths(IFileItem queueItem, string fileName)
        => new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName };

    private static string BuildAdAccountsReportFileName(APIReport<ReportSettings> report, IFileItem queueItem)
        => $"{queueItem.FileGUID}_{report.APIReportName}.{report.ReportSettings.FileExtension}".ToLower();

    private string BuildFilePath(IFileItem queueInfo, ApiReportItem apiReportItem)
        => $"{_baseDestUri.AbsoluteUri.TrimStart('/')}/{queueInfo.EntityID}/{GetDatedPartition(queueInfo.FileDate)}/{apiReportItem.FileName}".ToLower();

    private static bool IsAdCampaignGroupsReport(APIReport<ReportSettings> apiReport)
        => apiReport.APIReportName == ReportNames.AdCampaignGroups && apiReport.ReportSettings.ReportType == ReportTypes.Dimension;

    private static bool IsAdCampaignsReport(APIReport<ReportSettings> apiReport)
        => apiReport.APIReportName == ReportNames.AdCampaigns && apiReport.ReportSettings.ReportType == ReportTypes.Dimension;

    private static bool IsCreativesReport(APIReport<ReportSettings> apiReport)
        => apiReport.APIReportName == ReportNames.Creatives && apiReport.ReportSettings.ReportType == ReportTypes.Dimension;

    private static bool IsAdAccountsReport(APIReport<ReportSettings> apiReport)
        => apiReport.APIReportName == ReportNames.AdAccounts && apiReport.ReportSettings.ReportType == ReportTypes.Dimension;

    private void CreateManifestFiles(List<ApiReportItem> reportList, Queue queueItem)
    {
        List<FileCollectionItem> factReportFiles = GetReportFilesByType(reportList, ReportTypes.Fact);
        List<FileCollectionItem> dimensionReportFiles = GetReportFilesByType(reportList, ReportTypes.Dimension);

        List<FileCollectionItem> factManifestFiles = CreateFactManifestFiles(queueItem, factReportFiles);
        List<FileCollectionItem> dimensionManifestFiles =
            CreateDimensionsManifestFiles(queueItem, dimensionReportFiles);

        UpdateQueueItem(
            queueItem,
            reportList,
            rawMetricFiles: [.. factReportFiles, .. dimensionReportFiles],
            fileCollectionJson: BuildFileCollectionJson(factManifestFiles, dimensionManifestFiles));

        SaveDimensionDownloadedDateLookup();
        SaveDimensionDownloadedManifestFilesLookup();

        UpdateJobStatus(queueItem);
    }

    private static string BuildFileCollectionJson(List<FileCollectionItem> factManifestFiles, List<FileCollectionItem> dimensionManifestFiles)
    {
        return JsonConvert.SerializeObject(factManifestFiles.Concat(dimensionManifestFiles).ToList());
    }

    private List<FileCollectionItem> CreateFactManifestFiles(Queue queueItem, List<FileCollectionItem> factReportFiles)
    {
        return ETLProvider.CreateManifestFiles(queueItem,
            factReportFiles, _baseDestUri, GetDatedPartition).Select(file => new FileCollectionItem
            {
                SourceFileName = file.SourceFileName,
                FilePath = ETLProvider.GetManifestPath(queueItem, GetDatedPartition, file.FilePath, _baseDestUri).ToLower(),
                FileSize = file.FileSize
            }).ToList();
    }

    private List<FileCollectionItem> CreateDimensionsManifestFiles(Queue queueItem, List<FileCollectionItem> dimensionReportFiles)
    {
        List<FileCollectionItem> dimensionManifestFiles = ETLProvider.CreateManifestFiles(queueItem,
            dimensionReportFiles, _baseDestUri, GetDatedPartition);

        if (dimensionManifestFiles.Count == 0)
        {
            return _dimensionManifestFiles[queueItem.EntityID] ?? [];
        }

        _dimensionManifestFiles[queueItem.EntityID] = dimensionManifestFiles.Select(file => new FileCollectionItem
        {
            SourceFileName = file.SourceFileName,
            FilePath = ETLProvider.GetManifestPath(queueItem, GetDatedPartition, file.FilePath, _baseDestUri).ToLower(),
            FileSize = file.FileSize
        }).ToList();

        return _dimensionManifestFiles[queueItem.EntityID];
    }

    private static List<FileCollectionItem> GetReportFilesByType(List<ApiReportItem> reportList, string reportType)
    {
        return reportList
            .Where(x => x.ReportType == reportType)
            .Select(x => x.FileCollection)
            .ToList();
    }

    private static void UpdateQueueItem(
        Queue queueItem,
        List<ApiReportItem> reportList,
        List<FileCollectionItem> rawMetricFiles,
        string fileCollectionJson
    )
    {
        queueItem.FileCollectionJSON = fileCollectionJson;
        queueItem.FileSize = rawMetricFiles.Sum(x => x.FileSize);
        queueItem.DeliveryFileDate = reportList.Max(x => x.FileDate);
    }

    private static void UpdateJobStatus(Queue queueItem)
    {
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update(queueItem);
    }

    private void SaveLookup<T>(CompositeFormat compositeFormat, T lookupValue)
    {
        Lookup dbState = SetupService.GetById<Lookup>(string.Format(null, compositeFormat, CurrentIntegration.IntegrationID));

        if (dbState == null)
        {
            SetupService.InsertIntoLookup(string.Format(null, compositeFormat, CurrentIntegration.IntegrationID), JsonConvert.SerializeObject(lookupValue));
            return;
        }

        Lookup linkedInLookup = new()
        {
            Name = string.Format(null, compositeFormat, CurrentIntegration.IntegrationID),
            Value = JsonConvert.SerializeObject(lookupValue)
        };

        SetupService.Update(linkedInLookup);
    }

    private void SaveDimensionDownloadedManifestFilesLookup()
        => SaveLookup(_downloaded_dim_manifest_files, _dimensionManifestFiles);

    private void SaveDimensionDownloadedDateLookup()
        => SaveLookup(_downloaded_dim_reports, _apiEntitiesDownloadedDimensionReports);

    #endregion

    #region PreExecute Helpers
    private ApiClient CreateApiClient()
    {
        return new ApiClient(
            new ApiClientOptions
            {
                EndpointUri = CurrentIntegration.EndpointURI,
                PageSize = LookupService.GetLookupValueWithDefault(Constants.LINKEDIN_PAGE_SIZE, DefaultPageSize),
            },
            _httpClientProvider,
            new TokenApiClient(_httpClientProvider, _tokenCache, CurrentCredential));
    }

    private List<IFileItem> GetQueueItems(int nbTopResult)
    {
        return JobService.GetActiveOrderedTopQueueItemsBySource(
            sourceID: CurrentSource.SourceID,
            nbResults: nbTopResult,
            jobLogID: JobLogger.JobLog.JobLogID,
            integrationID: CurrentIntegration.IntegrationID)?.ToList() ?? new List<IFileItem>();
    }

    private Dictionary<string, DateTime?> GetAllReportsToDownload()
    {
        Lookup dimensionReportsLookup = SetupService.GetById<Lookup>(string.Format(null, _downloaded_dim_reports, CurrentIntegration.IntegrationID));

        return string.IsNullOrEmpty(dimensionReportsLookup?.Value)
            ? new Dictionary<string, DateTime?>()
            : ETLProvider.DeserializeType<Dictionary<string, DateTime?>>(dimensionReportsLookup.Value);
    }

    private Dictionary<string, List<FileCollectionItem>> GetAllDimensionManifestFiles()
    {
        Lookup lookup = SetupService.GetById<Lookup>(string.Format(null, _downloaded_dim_manifest_files,
            CurrentIntegration.IntegrationID));

        return string.IsNullOrEmpty(lookup?.Value)
            ? []
            : ETLProvider.DeserializeType<Dictionary<string, List<FileCollectionItem>>>(lookup.Value);
    }

    #endregion

    #region Exceptions

    private void HandleExceptions()
    {
        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
    }
    private void HandleHttpRequestException(IFileItem queueItem, HttpClientProviderRequestException ex)
    {
        LogHttpRequestException(queueItem, ex);
        IncrementExceptionCountAndUpdateStatus(queueItem);
    }
    private void HandleGeneralException(IFileItem queueItem, Exception ex)
    {
        LogException(queueItem, ex);
        IncrementExceptionCountAndUpdateStatus(queueItem);
    }

    private void IncrementExceptionCountAndUpdateStatus(IFileItem queueItem)
    {
        _exceptionCount++;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
    }

    #endregion

    #region Logs 

    private void LogPreExecuteInfo()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));

    private void LogDimensionDataAlreadyDownloaded(IFileItem queueItem)
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"Dimension data for: {queueItem.EntityID} already downloaded today. Skipping.")));

    private void LogJobComplete()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("Import job complete")));

    private void LogRuntimeExceeded()
        => _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid($"Import job error -> Exception: The runtime ({_runtime.Elapsed}) exceeded the allotted time {_maxRuntime}")));

    private void LogNoReportsInQueue()
        => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("There are no reports in the Queue")));
    private void LogHttpRequestException(IFileItem queueItem, HttpClientProviderRequestException ex)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Failed to download report -> failed on: {queueItem.ID} " +
                          $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> " +
                          $"|Exception details : {ex}"), ex));
    }
    private void LogException(IFileItem queueItem, Exception ex)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
            PrefixJobGuid($"Failed to download report -> failed on: {queueItem.ID} " +
                          $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  ->" +
                          $"Error -> Exception: {ex.Message} -> STACK {ex.StackTrace}"), ex));
    }
    private void LogFactReportDownloadError(IFileItem queueItem, HttpClientProviderRequestException ex)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                                PrefixJobGuid($"Error downloading report - failed on queueID: {queueItem.ID} " +
                                              $"for EntityID: {queueItem.EntityID} " +
                                              $"FileDate: {queueItem.FileDate} Fact Report" +
                                              $"|Exception details : {ex}"), ex));
    }
    private void LogFactReportDownloadError(IFileItem queueItem, Exception ex)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                                PrefixJobGuid($"Error downloading report - failed on queueID: {queueItem.ID} " +
                                              $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Fact Report" +
                                              $"Error -> Exception: {ex.Message} -> STACK {ex.StackTrace}"), ex));
    }

    private void LogNoFactReportsFound(IFileItem queueItem)
    {
        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                                PrefixJobGuid($"Failed to download fact reports for queueID: {queueItem.ID} -> EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}. No ReportLists found.")));
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

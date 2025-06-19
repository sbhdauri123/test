using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.SearchAds360;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Queue = Greenhouse.Data.Model.Core.Queue;

namespace Greenhouse.Jobs.Aggregate.SearchAds360;

[Export("GenericDTSImportJob", typeof(IDragoJob))]
public class DTSImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private readonly static Logger _logger = NLog.LogManager.GetCurrentClassLogger();
    private Uri _baseDestUri;
    private Action<LogLevel, string> _log;
    private Action<LogLevel, string, Exception> _logEx;
    private Action<string> _logWarn;
    private int _maxPollyRetry;
    private int _maxS3PollyRetry;
    private int _s3PauseGetLength;
    private TimeSpan _maxRuntime;
    private RemoteAccessClient _googleClient;
    private int _exceptionCounter;
    private int _warningCounter;
    private readonly Stopwatch _runtime = new();
    private readonly Dictionary<string, RegexCodec> _regexCodexCache = new();
    private int _totalLookbackDays;
    private int _maxDegreeOfParallelism;
    private const string DONE_FOLDER_NAME = "Done";
    private DimensionState _dimensionState;
    private string _dimensionDateLookupKey;

    public void PreExecute()
    {
        Stage = Constants.ProcessingStage.RAW;
        Initialize();
        _baseDestUri = GetDestinationFolder();
        _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
        _logEx = (logLevel, msg, ex) => { _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex)); _exceptionCounter++; };
        _logWarn = (msg) => { _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(msg))); _warningCounter++; };
        _log(LogLevel.Info, $"{CurrentSource.SourceName} - IMPORT-PREEXECUTE {DefaultJobCacheKey}");
        _maxPollyRetry = LookupService.GetGlobalLookupValueWithDefault(Constants.GENERIC_GCS_IMPORT_MAX_RETRY, CurrentSource.SourceID, 3);
        _maxS3PollyRetry = LookupService.GetGlobalLookupValueWithDefault(Constants.GENERIC_GCS_IMPORT_S3_MAX_RETRY, CurrentSource.SourceID, 3);
        _s3PauseGetLength = int.Parse(SetupService.GetById<Lookup>(Constants.S3_PAUSE_GETLENGTH).Value);
        _maxRuntime = LookupService.GetGlobalLookupValueWithDefault(Constants.GENERIC_GCS_IMPORT_MAX_RUNTIME, CurrentSource.SourceID, new TimeSpan(0, 3, 0, 0));
        _googleClient = new RemoteAccessClient(new Uri($"{CurrentIntegration.EndpointURI}"), CurrentCredential);
        _totalLookbackDays = LookupService.GetLookupValueWithDefault(Constants.GENERIC_GCS_IMPORT_TOTAL_LOOKBACK_DAYS, 30);
        _maxDegreeOfParallelism = LookupService.GetLookupValueWithDefault(Constants.GENERIC_GCS_IMPORT_MAX_DEGREE_PARALLELISM, 3);
        _dimensionDateLookupKey = $"{Constants.GENERIC_GCS_IMPORT_LATEST_DIMENSION_DATE}_{CurrentIntegration.IntegrationID}";
        _dimensionState = LookupService.GetAndDeserializeLookupValueWithDefault(_dimensionDateLookupKey, new DimensionState());
    }

    public void Execute()
    {
        _runtime.Start();

        try
        {
            _log(LogLevel.Info, $"Start Import for Integration: {CurrentIntegration.IntegrationName}");

            string entityID = GetEntityID();

            IEnumerable<IDirectory> dateDirectoriesInCloud = GetDirectories();

            DownloadDimensionFiles(entityID, dateDirectoriesInCloud);

            DownloadDeliveryFiles(entityID, dateDirectoriesInCloud);
        }
        catch (Exception exc)
        {
            _logEx(LogLevel.Error, $"Global Catch on Execute-> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
        }

        if (_exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors regular: {_exceptionCounter}; Please check Splunk for more detail.");
        }
        else if (_warningCounter > 0)
        {
            JobLogger.JobLog.Status = nameof(Constants.JobLogStatus.Warning);
            JobLogger.JobLog.Message = $"Total warnings: {_warningCounter}; For full list search for Warnings in splunk";
        }

        _log(LogLevel.Info, "Import job complete");
    }

    private void DownloadDeliveryFiles(string entityID, IEnumerable<IDirectory> dateDirectoriesInCloud)
    {
        List<(DateTime eventDate, DateTime lastModified)> reissuedDates = GetReissuedDates(dateDirectoriesInCloud);

        var missingDates = GetMissingDates(dateDirectoriesInCloud);
        var reissuedEventDates = reissuedDates.Select(x => x.eventDate);
        var datesToDownload = reissuedEventDates.Union(missingDates).OrderBy(d => d);
        if (!datesToDownload.Any())
        {
            _log(LogLevel.Info, $"There are no new dates to download for delivery today {DateTime.Today}.");
            return;
        }

        foreach (var date in datesToDownload)
        {
            if (IsMaxRuntime())
            {
                break;
            }

            try
            {
                _log(LogLevel.Info, $"Start importing date: {date:yyyy-MM-dd} for Entity ID: {entityID}");

                IEnumerable<IFile> remoteFiles = GetFilesDetailsFromCloud(date);

                DateTime? doneFileDate = GetDoneFileLastModifiedDate(remoteFiles);

                if (!FilesAreDone(doneFileDate, remoteFiles))
                {
                    _log(LogLevel.Info, $"SKIPPING IMPORT - Files are not done in GCS for Entity ID: {entityID} for date: {date}");
                    continue;
                }

                (IEnumerable<IFile> filteredFiles, bool matchAllSourceFille) = FilterBySourceFileRegex(remoteFiles, date);

                if (!matchAllSourceFille)
                {
                    _log(LogLevel.Info, $"Not all files present in GCS for Entity ID: {entityID} for date: {date}");
                    continue;
                }

                DownloadFilesAndCreateQueue(entityID, date, filteredFiles, doneFileDate.Value);
            }
            catch (HttpClientProviderRequestException ex)
            {
                _logEx(LogLevel.Error, $"Error downloading report - failed on Integration: " +
                        $"{CurrentIntegration.IntegrationName} for EntityID: {entityID} " +
                        $"|Exception details : {ex}", ex);
                break;
            }
            catch (Exception exc)
            {
                _logEx(LogLevel.Error, $"Error downloading report - failed on Integration: {CurrentIntegration.IntegrationName} for EntityID: {entityID} " +
                        $"  - Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                break;
            }
        }
    }

    private void DownloadDimensionFiles(string entityID, IEnumerable<IDirectory> dateDirectoriesInCloud)
    {
        var reissuedDoneFileDates = GetReissuedDoneFileDates(dateDirectoriesInCloud, new List<IFileItem>());

        if (reissuedDoneFileDates.Count == 0)
        {
            _log(LogLevel.Info, "Skipping Dimension download - no reissue dates found");
            return;
        }

        try
        {
            if (IsMaxRuntime())
            {
                return;
            }

            var (eventDate, doneLastModified) = reissuedDoneFileDates.OrderByDescending(x => x.eventDate).FirstOrDefault();

            if (_dimensionState.LatestDateImported.Date.Equals(eventDate.Date) && _dimensionState.DoneLastModified.CompareWithoutMilliseconds(doneLastModified) >= 0)
            {
                _log(LogLevel.Info, $"DIMENSION - Skipping download - latest dimension date {_dimensionState.LatestDateImported:yyyy-MM-dd} is current: {_dimensionState.DoneLastModified} - done file date: {doneLastModified}");
                return;
            }

            _log(LogLevel.Info, $"DIMENSION - Start importing date: {eventDate:yyyy-MM-dd} (lastModified:{doneLastModified}) for Entity ID: {entityID}");

            IEnumerable<IFile> remoteFiles = GetFilesDetailsFromCloud(eventDate);

            (IEnumerable<IFile> filteredFiles, bool matchAllSourceFille) = FilterBySourceFileRegex(remoteFiles, eventDate, true);

            if (!matchAllSourceFille)
            {
                _log(LogLevel.Info, $"DIMENSION - Not all files present in GCS for Entity ID: {entityID} for date: {eventDate}");
                return;
            }

            DownloadFilesAndCreateQueue(entityID, eventDate, filteredFiles, doneLastModified, true);

            _dimensionState = new() { LatestDateImported = eventDate, DoneLastModified = doneLastModified };
            LookupService.SaveJsonObject(_dimensionDateLookupKey, _dimensionState);

            _log(LogLevel.Info, $"DIMENSION - END importing date: {eventDate:yyyy-MM-dd} for Entity ID: {entityID}");
        }
        catch (Exception exc)
        {
            _logEx(LogLevel.Error, $"DIMENSION - Error downloading report - failed on Integration: {CurrentIntegration.IntegrationName} for EntityID: {entityID} " +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
        }
    }

    private bool IsMaxRuntime()
    {
        bool isMaxRuntime = false;

        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
        {
            _logWarn($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
            isMaxRuntime = true;
        }

        return isMaxRuntime;
    }

    private string GetEntityID()
    {
        return CurrentIntegration.EndpointURI.Split("/").Last();
    }

    private RegexCodec GetRegexCodec(string mask)
    {
        if (_regexCodexCache.TryGetValue(mask, out RegexCodec value))
        {
            return value;
        }

        var regexCodex = new RegexCodec(mask);
        _regexCodexCache.Add(mask, regexCodex);
        return regexCodex;
    }

    private IEnumerable<IFile> GetFilesDetailsFromCloud(DateTime date, string folderName = null)
    {
        RegexCodec regexIntegration = GetRegexCodec(CurrentIntegration.RegexMask);

        Uri currentUri = GetDtsRemoteUri(date, folderName);
        IEnumerable<IFile> remoteFiles = Enumerable.Empty<IFile>();

        var downloadPolicy = new ExponentialBackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxPollyRetry
        };

        var downloadRetry = new CancellableRetry(JED.JobGUID.ToString(), downloadPolicy, _runtime, _maxRuntime);
        downloadRetry.Execute(() =>
        {
            remoteFiles = _googleClient.WithDirectory(currentUri, base.HttpClientProvider)
                                       .GetFiles(true)
                                       .Where(remoteFile => regexIntegration.FileNameRegex.IsMatch(remoteFile.Name));
        });

        return remoteFiles;
    }

    private Uri GetDtsRemoteUri(DateTime date, string folderName)
    {
        string[] paths = new string[] { date.ToString("yyyy-MM-dd"), folderName ?? string.Empty };
        return RemoteUri.CombineUri(new Uri(CurrentIntegration.EndpointURI), paths);
    }

    private void DownloadFilesAndCreateQueue(string entityID, DateTime dataDate, IEnumerable<IFile> remoteFiles, DateTime doneFileDate, bool isDimension = false)
    {
        _log(LogLevel.Info, $"Start Downloading files for date: {dataDate:yyyy-MM-dd} for Entity ID: {entityID} - total files: {remoteFiles.Count()}");
        Guid fileguid = Guid.NewGuid();

        ConcurrentQueue<long> fileSizes = new();
        ConcurrentQueue<DateTime> lastUpdatedDates = new();
        ConcurrentQueue<Exception> exceptions = new();
        ConcurrentBag<string> corruptedFiles = new();
        Parallel.ForEach(remoteFiles, new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, (file, state) =>
        {
            try
            {
                (long thisFileSize, DateTime thisLastUpdated, bool fileCorrupted) = StreamFileToS3((GCSFile)file, fileguid, dataDate, entityID);

                if (fileCorrupted)
                {
                    _logWarn($"{fileguid}-Corrupt file - stopping current download of:{file.FullName} for entityID:{entityID} and event-date:{dataDate}");
                    corruptedFiles.Add(file.FullName);
                    state.Stop();
                    return;
                }

                lastUpdatedDates.Enqueue(thisLastUpdated);
                fileSizes.Enqueue(thisFileSize);
            }
            catch (HttpClientProviderRequestException ex)
            {
                exceptions.Enqueue(ex);
                state.Stop();
            }
            catch (Exception ex)
            {
                _logEx(LogLevel.Error, $"{fileguid}-Download failed - {file.FullName}|Exception:{ex.GetType().FullName}|Message:{ex.Message}|InnerExceptionMessage:{ex.InnerException?.Message}", ex);
                exceptions.Enqueue(ex);
                state.Stop();
            }
        });

        if (!corruptedFiles.IsEmpty)
        {
            _log(LogLevel.Info, $"{fileguid}-Files may be corrupted; skipping remaining downloads and queue creation - Files are not done in GCS for Entity ID: {entityID} for date: {dataDate}");
            return;
        }

        if (!exceptions.IsEmpty)
        {
            ExceptionDispatchInfo.Capture(exceptions.First()).Throw();
        }

        long fileSize = fileSizes.IsEmpty ? 0 : fileSizes.Sum();

        CreateQueue(fileguid, dataDate, entityID, fileSize, doneFileDate, isDimension);
    }

    private (IEnumerable<IFile> filteredFiles, bool matchAllSourceFile) FilterBySourceFileRegex(IEnumerable<IFile> remoteFiles, DateTime date, bool isDimension = false)
    {
        List<IFile> filteredFiles = new();
        bool matchAll = true;

        var sourceFiles = SourceFiles.Where(x => !x.IsDoneFile && x.HasDeliveryData == !isDimension).ToList();

        if (sourceFiles.Count == 0)
        {
            return (filteredFiles, false);
        }

        var doneSourceFile = SourceFiles.FirstOrDefault(x => x.IsDoneFile);
        if (doneSourceFile == null)
        {
            _log(LogLevel.Error, $"Missing done source-file (IsDoneFile = true) - Entity ID: {GetEntityID()} for date: {date}");
            return (filteredFiles, false);
        }

        // ETL peeks at the date within done file, so always need to include in file collection
        if (!sourceFiles.Contains(doneSourceFile))
        {
            sourceFiles.Add(doneSourceFile);
        }

        foreach (SourceFile expectedFile in sourceFiles)
        {
            IEnumerable<IFile> matches = GetFilesMatchingRegex(remoteFiles, expectedFile.RegexMask);

            if (!matches.Any())
            {
                _log(LogLevel.Debug, $"No matching files found for {expectedFile.SourceFileName} - Entity ID: {GetEntityID()} for date: {date} - regex mask: {expectedFile.RegexMask}");
                matchAll = false;
                break;
            }

            filteredFiles.AddRange(matches);
        }

        return (filteredFiles, matchAll);
    }

    private IEnumerable<IFile> GetFilesMatchingRegex(IEnumerable<IFile> remoteFiles, string regexMask)
    {
        RegexCodec regexExpectedFile = GetRegexCodec(regexMask);
        return remoteFiles.Where(file => regexExpectedFile.FileNameRegex.IsMatch($"{file.Directory.Name}/{file.Name}"));
    }

    private (long fileSize, DateTime lastUpdated, bool fileCorrupted) StreamFileToS3(GCSFile cloudFile, Guid fileguid, DateTime dateToProcess, string entityID)
    {
        bool fileCorrupted = false;

        string date = dateToProcess.ToString("yyyy-MM-dd");

        string reportType = cloudFile.Directory.Name;
        var fileName = $"{fileguid}_{entityID}_{reportType}_{date}_{cloudFile.FullName.Split("/").Last()}";
        string[] paths = new string[]
        {
            entityID, GetDatedPartition(dateToProcess),fileName
        };
        Uri destUri = RemoteUri.CombineUri(_baseDestUri, paths);
        var destFile = new S3File(destUri, GreenhouseS3Creds);

        var s3Policy = new ExponentialBackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxS3PollyRetry
        };

        using CancellationTokenSource cancellationTokenSource = new();
        var s3Retry = new CancellableRetry(JED.JobGUID.ToString(), s3Policy, _runtime, _maxRuntime);
        s3Retry.Execute(() =>
        {
            try
            {
                UploadToS3(cloudFile, (S3File)destFile, paths);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.PreconditionFailed)
            {
                _log(LogLevel.Error, $"GCS File {cloudFile.FullName} does not match the expected generation number {cloudFile.Generation}.");
                fileCorrupted = true;
                cancellationTokenSource.Cancel();
            }
        }, cancellationTokenSource.Token);

        //from time to time S3 will return the wrong file size
        //pausing has proven to reduce the probability of this issue happening
        Task.Delay(_s3PauseGetLength).Wait();

        return (destFile.Length, cloudFile.LastWriteTimeUtc, fileCorrupted);
    }

    private static DateTime? ExtractDateOrNull(string directory) => DateTime.TryParseExact(directory, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime value) ? value : null;

    private IEnumerable<DateTime> GetMissingDates(IEnumerable<IDirectory> dateDirectoriesInCloud)
    {
        var alreadyImported = JobService.GetDistinctFileLogDateAndHour(CurrentIntegration.IntegrationID);
        var alreadyImportedDates = alreadyImported.Select(a => a.FileDate);

        IEnumerable<DateTime> datesinCloud = GetDatesFromCloudDirectories(dateDirectoriesInCloud);

        var startDate = CurrentIntegration.FileStartDate;

        return datesinCloud.Where(d => d >= startDate)
                                        .Except(alreadyImportedDates);
    }

    private static IEnumerable<DateTime> GetDatesFromCloudDirectories(IEnumerable<IDirectory> dateDirectoriesInCloud)
    {
        return dateDirectoriesInCloud.Select(d => ExtractDateOrNull(d.Name.Trim('/')))
                                                 .Where(d => d.HasValue)
                                                 .Select(d => d.Value);
    }

    private IEnumerable<IDirectory> GetDirectories()
    {
        var downloadPolicy = new ExponentialBackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxPollyRetry
        };
        var downloadRetry = new CancellableRetry(JED.JobGUID.ToString(), downloadPolicy, _runtime, _maxRuntime);
        IEnumerable<IDirectory> directories = Enumerable.Empty<IDirectory>();
        var cloudEndpoint = new Uri($"{CurrentIntegration.EndpointURI.TrimEnd('/')}/");

        downloadRetry.Execute(() => directories = _googleClient.WithDirectory(cloudEndpoint, base.HttpClientProvider).GetDirectories());

        return directories;
    }

    private void CreateQueue(Guid fileguid, DateTime dataDate, string entityID, long fileSize, DateTime lastUpdated, bool isDimension)
    {
        _log(LogLevel.Info, $"Creating queue with fileguid: {fileguid} for date: {dataDate:yyyy-MM-dd} for Entity ID: {entityID}");
        var importFile = new Queue()
        {
            FileGUID = fileguid,
            FileSize = fileSize,
            IntegrationID = CurrentIntegration.IntegrationID,
            SourceID = CurrentSource.SourceID,
            Status = nameof(Constants.JobStatus.Complete),
            StatusId = (int)Constants.JobStatus.Complete,
            JobLogID = JobLogger.JobLog.JobLogID,
            Step = nameof(Constants.JobStep.Import),
            FileDate = dataDate.Date,
            DeliveryFileDate = lastUpdated,
            EntityID = entityID,
            SourceFileName = CurrentSource.SourceName,
            FileName = $"{CurrentSource.SourceName}_{dataDate:yyyyMMdd}_{entityID}",
            IsDimOnly = isDimension
        };

        JobService.Add<IFileItem>(importFile);
    }

    #region File-Reissue-Date Helpers
    private DateTime GetLookbackUtcStartDate()
    {
        return DateTime.UtcNow.Date.AddDays(-_totalLookbackDays);
    }

    private static DateTime GetYesterdayUtcDate()
    {
        return DateTime.UtcNow.Date.AddDays(-1);
    }

    private List<(DateTime eventDate, DateTime lastModified)> GetReissuedDates(IEnumerable<IDirectory> dateDirectoriesInCloud)
    {
        List<(DateTime eventDate, DateTime lastModified)> reissuedDates = new();

        var queuesBasedOnLookback = JobService.GetLatestDeliveryFileDate(CurrentIntegration.IntegrationID, GetLookbackUtcStartDate());

        var reissuedDoneFileDates = GetReissuedDoneFileDates(dateDirectoriesInCloud, queuesBasedOnLookback);

        if (reissuedDoneFileDates.Count != 0)
        {
            reissuedDates.AddRange(reissuedDoneFileDates);
        }

        return reissuedDates;
    }

    private IEnumerable<DateTime> GenerateLookbackDates()
    {
        DateTime? startDate = UtilsDate.GetLatestDateTime(GetLookbackUtcStartDate(), CurrentIntegration.FileStartDate);
        return UtilsDate.BuildDateRange(startDate.Value, GetYesterdayUtcDate());
    }

    #region Done File Helpers
    private List<(DateTime eventDate, DateTime lastModified)> GetReissuedDoneFileDates(IEnumerable<IDirectory> dateDirectoriesInCloud, IEnumerable<IFileItem> queuesBasedOnLookback)
    {
        List<(DateTime eventDate, DateTime lastModified)> doneFileDates = new();

        var lookbackDates = GenerateLookbackDates();

        if (!lookbackDates.Any())
        {
            _logWarn($"Fail to generate Lookback dates - cannot check which done files were reissued in DTS. Please update lookup name: GENERIC_GCS_IMPORT_TOTAL_LOOKBACK_DAYS; current lookup value: {_totalLookbackDays}");
            return doneFileDates;
        }

        var eventDates = GetDatesFromCloudDirectories(dateDirectoriesInCloud).Intersect(lookbackDates).ToList();

        eventDates.ForEach(eventDate =>
        {
            var matchingQueue = queuesBasedOnLookback.FirstOrDefault(x => x.FileDate.Date == eventDate.Date);

            var (isReissued, lastModified) = IsDoneFileReissued(eventDate, matchingQueue);
            if (isReissued)
            {
                doneFileDates.Add((eventDate, lastModified));
            }
        });

        return doneFileDates;
    }

    private (bool isReissued, DateTime lastModified) IsDoneFileReissued(DateTime eventDate, IFileItem queueItem)
    {
        bool isReissued = false;
        DateTime lastModifiedDate = DateTime.UnixEpoch;

        try
        {
            IFile doneFile = GetFilesDetailsFromCloud(eventDate, DONE_FOLDER_NAME).FirstOrDefault();

            if (doneFile == null)
            {
                _logWarn($"DONE FILE MISSING in GCS for Entity ID: {GetEntityID()} for date: {eventDate:yyyy-MM-dd} - fail to check if done file has been reissued");
                return (false, lastModifiedDate);
            }

            if (queueItem?.DeliveryFileDate?.CompareWithoutMilliseconds(doneFile.LastWriteTimeUtc) >= 0)
            {
                _log(LogLevel.Debug, $"Skipping date {eventDate:yyyy-MM-dd} - done file NOT reissued -  latest delivery file date is current: {queueItem.DeliveryFileDate} - done file date: {doneFile.LastWriteTimeUtc}");
            }
            else
            {
                isReissued = true;
                lastModifiedDate = doneFile.LastWriteTimeUtc;
            }
        }
        catch (Exception exc)
        {
            _logEx(LogLevel.Error, $"Error getting done file - failed on Integration: {CurrentIntegration.IntegrationName} for EntityID: {GetEntityID()} date: {eventDate:yyyy-MM-dd}" +
                    $"  - Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
        }

        return (isReissued, lastModifiedDate);
    }

    private DateTime? GetDoneFileLastModifiedDate(IEnumerable<IFile> remoteFiles)
    {
        SourceFile doneSourceFile = SourceFiles.FirstOrDefault(x => x.IsDoneFile);
        if (doneSourceFile == null)
        {
            return null;
        }

        IFile doneFile = GetFilesMatchingRegex(remoteFiles, doneSourceFile.RegexMask).FirstOrDefault();

        if (doneFile == null)
        {
            return null;
        }

        return doneFile.LastWriteTimeUtc;
    }

    private bool FilesAreDone(DateTime? doneFileDate, IEnumerable<IFile> remoteFiles)
    {
        bool isDone = true;

        if (!doneFileDate.HasValue)
        {
            _log(LogLevel.Debug, "Done File is missing.");
            return false;
        }

        var mostRecentModifiedDate = remoteFiles.Max(x => x.LastWriteTimeUtc);

        if (mostRecentModifiedDate.CompareWithoutMilliseconds(doneFileDate.Value) > 0)
        {
            _log(LogLevel.Debug, $"Files are currently being reissued - most recent modified date {mostRecentModifiedDate} is greater than the done file date: {doneFileDate.Value}");
            isDone = false;
        }

        return isDone;
    }

    #endregion
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
            _googleClient?.Dispose();
        }
    }

    ~DTSImportJob()
    {
        Dispose(false);
    }
}

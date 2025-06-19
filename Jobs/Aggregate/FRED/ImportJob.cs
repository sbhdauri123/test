using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.FRED;
using Greenhouse.Data.DataSource.FRED;
using Greenhouse.Data.DataSource.FRED.Series;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.FRED;

[Export("FRED-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Framework.BaseFrameworkJob, IDragoJob
{
    private readonly static Logger _logger = LogManager.GetCurrentClassLogger();
    private const string RateLimitHeaderRemaining = "x-rate-limit-remaining"; // # of calls remaining in the current minute
    private RemoteAccessClient _remoteAccessClient;
    private Uri _baseDestUri;
    private Uri _baseStageDestUri;
    private Uri _baseLocalImportUri;
    private List<OrderedQueue> _queueItems;
    private IEnumerable<APIReport<ObservationParameters>> _observationReports;
    private List<SeriesState> _seriesState;
    private SeriesState _currentSeriesState;
    private int _callThreshold;
    private int _maxRetry;
    private int _millisecondsDelay;
    private int _exceptionCount;
    private int _warningCount;
    private readonly Stopwatch _runtime = new();
    private TimeSpan _maxRuntime;
    private string JobGUID
    {
        get { return this.JED.JobGUID.ToString(); }
    }
    private Action<LogLevel, string> _log;
    private Action<LogLevel, string, Exception> _logEx;

    public void PreExecute()
    {
        _log = (logLevel, msg) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg)));
        _logEx = (logLevel, msg, ex) => _logger.Log(Msg.Create(logLevel, _logger.Name, PrefixJobGuid(msg), ex));
        _log(LogLevel.Info, $"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _baseStageDestUri = new Uri(_baseDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _baseLocalImportUri = GetLocalImportDestinationFolder();
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID).OrderBy(q => q.RowNumber).ToList();
        _remoteAccessClient = base.GetS3RemoteAccessClient();
        //Available reports:
        //1) FRED series - observations: Get the observations or data values for an economic data series. One report per "series ID":
        //7 API reports: 7 metrics (UNRATE, DSPI, CPI, POILBREUSDM, UMCSENT, GDP, GDPC1, SP500)
        _observationReports = JobService.GetAllActiveAPIReports<ObservationParameters>(CurrentSource.SourceID);
        if (!_observationReports.Any())
        {
            throw new APIReportException($"No active API Reports found for {CurrentSource.SourceID}");
        }

        _seriesState = LookupService.GetAndDeserializeLookupValueWithDefault(Constants.FRED_SERIES_STATE, new List<SeriesState>());
        //call threshold within the value in this rate limit header "x-rate-limit-remaining" to avoid 429 Error
        _callThreshold = LookupService.GetLookupValueWithDefault(Constants.FRED_API_CALL_THRESHOLD, 10);
        _maxRetry = LookupService.GetLookupValueWithDefault(Constants.FRED_POLLY_MAX_RETRY, 10);
        _maxRuntime = LookupService.GetLookupValueWithDefault(Constants.FRED_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        _millisecondsDelay = LookupService.GetLookupValueWithDefault(Constants.FRED_MILLISECONDS_DELAY_BETWEEN_CALLS, 300);
    }

    public void Execute()
    {
        _log(LogLevel.Info, $"EXECUTE START {base.DefaultJobCacheKey}");

        if (_queueItems.Count == 0)
        {
            _log(LogLevel.Info, "No items in the Queue");
            return;
        }

        _runtime.Start();

        foreach (Queue queueItem in _queueItems)
        {
            JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
            try
            {
                CleanupLocalEntityFolder(queueItem);

                foreach (var report in _observationReports)
                {
                    if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) throw new TimeoutException();

                    //create local file to store raw json
                    var localFile = CreateLocalFile(queueItem, report);

                    //1) Date Range Filter
                    // daily job will retrieve data from the integration-file-start-date thru "today"
                    // backfill job will be the same as daily for now
                    report.ReportSettings.ObservationStart ??= $"{CurrentIntegration.FileStartDate:yyyy-MM-dd}";

                    // for backfill, we will use the file date as the end date, otherwise, we take what is set up in the API Report, which could be NULL. This is okay because the default for "observation_end" is "9999-12-31 (latest available)"
                    report.ReportSettings.ObservationEnd = queueItem.IsBackfill ? $"{queueItem.FileDate:yyyy-MM-dd}" : report.ReportSettings.ObservationEnd;

                    // check if the observation_start is a date after the observation_end, which will throw an exception in the API
                    if (DateTime.Parse(report.ReportSettings.ObservationStart) > DateTime.Parse(report.ReportSettings.ObservationEnd ?? $"{DateTime.UtcNow}"))
                    {
                        _log(LogLevel.Debug, $"Observation_start date of {report.ReportSettings.ObservationStart} cannot be set after the observation_end date of {report.ReportSettings.ObservationEnd}; " +
                            "resetting observation_start to match observation_end");
                        report.ReportSettings.ObservationStart = report.ReportSettings.ObservationEnd;
                    }

                    var isScheduled = true;
                    if (!queueItem.IsBackfill)
                    {
                        //2) Schedule: Job runs daily, but API call is based on the Source:
                        // Examples: 2a) S & P 500: Weekday(Monday - Friday) ~~ ReportScheduleDetails:[{triggerDay:MONDAY},{triggerDay:TUESDAY},{triggerDay:WEDNESDAY},{triggerDay:THURSDAY},{triggerDay:FRIDAY}]
                        // 2b) Unemployment Rate: First Saturday ~~ ReportScheduleDetails:[{triggerDay:SATURDAY, interval:FIRST}]
                        // The examples above results in a string that will be used as a "regex" that will be checked to see if it matches an expression based on today(utc)
                        isScheduled = IsSchedule(report);
                    }

                    if (isScheduled)
                    {
                        DownloadReport(queueItem, report, localFile);
                        // for Daily jobs, update SeriesState Lookup and set DeltaDate to today's date
                        if (!queueItem.IsBackfill)
                        {
                            _currentSeriesState.DeltaDate = DateTime.UtcNow.Date;
                        }
                    }
                    else
                    {
                        _log(LogLevel.Info, $"Series id {report.ReportSettings.SeriesId} is not scheduled to run today");
                        //create empty file even if it is not scheduled to avoid redshift processing failure
                        localFile.Create().Dispose();
                    }

                    _log(LogLevel.Info, $"Data has been saved to a local file at {localFile.FullName}; size={localFile.Length}");

                    var files = queueItem.FileCollection?.ToList() ?? new List<FileCollectionItem>();

                    FileCollectionItem fileItem = new()
                    {
                        FileSize = localFile.Length,
                        SourceFileName = report.ReportSettings.SeriesId,
                        FilePath = localFile.Name
                    };
                    files.Add(fileItem);
                    queueItem.FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(files);
                    queueItem.FileSize += localFile.Length;
                }

                //convert json files to a format compatible with Redshift
                //and upload to stage folder in s3 for processing
                //local raw json files are archived in a tar.gz file and uploaded to raw s3 folder
                StageReport(queueItem);
            }
            catch (TimeoutException)
            {
                _warningCount++;
                _log(LogLevel.Warn, $"Runtime exceeded time allotted - {_runtime.ElapsedMilliseconds}ms; resetting status for {queueItem.FileGUID} back to Pending");
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Pending);
                break;
            }
            catch (AggregateException ae) // HttpClient is an async call that is called syncronously; exception thrown is an Aggregate Exception
            {
                queueItem.Status = Constants.JobStatus.Error.ToString();
                queueItem.StatusId = (int)Constants.JobStatus.Error;
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                foreach (var ex in ae.InnerExceptions)
                {
                    _logEx(LogLevel.Error, $"Error with queue item -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  ->Aggregate-InnerException: {ex.Message} - STACK {ex.StackTrace}", ex);
                    _exceptionCount++;
                }
                break;
            }
            catch (HttpClientProviderRequestException exc)
            {
                HandleException(queueItem, exc);
                break;
            }
            catch (Exception exc)
            {
                HandleException(queueItem, exc);
                break;
            }
        }

        _runtime.Stop();

        if (_exceptionCount > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
        }
        else if (_warningCount > 0)
        {
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = $"Total warnings: {_warningCount}; For full list search for Warnings in splunk";
        }

        _log(LogLevel.Info, $"EXECUTE END {base.DefaultJobCacheKey}");
    }
    private void HandleException<TException>(Queue queueItem, TException ex) where TException : Exception
    {
        _exceptionCount++;
        queueItem.Status = Constants.JobStatus.Error.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);

        // Build log message
        var logMessage = BuildLogMessage(queueItem, ex);
        _logEx(LogLevel.Error, logMessage, ex);
        // all or nothing: if a report fails, we remove all the other reports for that queue
        // and break the report loop to not go through the following reports
    }

    private static string BuildLogMessage<TException>(Queue queueItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Error with queue item -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} " +
                    $"FileDate: {queueItem.FileDate} -> Exception details : {httpEx}",
            _ =>
                $"Error with queue item -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }
    private void DownloadReport(Queue queueItem, APIReport<ObservationParameters> report, FileSystemFile localFile)
    {
        _log(LogLevel.Debug, $"Series id {report.ReportSettings.SeriesId} is scheduled to run today - starting data download");

        //FRED api uses "offset" and "limit" parameters to filter the response to a subset of the entire array of "observations". Also, FRED provides the "count" of the total records matching the api request criteria
        //"offset" will retrieve the records beginning at that position in a zero-based index array
        //to page through the results, we can increment the offset by a factor of the "limit" parameter, so if there are 30 records total and our limit is 10, then we want our "offset" to be 0, 10, and 20
        //we keep track of how many records we have requested in "recordsRequested" and can end the loop when we reach the "count" that FRED provides

        var recordsRequested = 0;
        var notDone = false;
        var counter = 0;

        do
        {
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) throw new TimeoutException();

            report.ReportSettings.Offset = recordsRequested.ToString();
            var observationRequest = new ApiRequest(CurrentIntegration.EndpointURI, JobGUID, CurrentCredential.CredentialSet.APIKey, report.ReportSettings.URLPath, HttpClientProvider);
            observationRequest.SetParameters(report.ReportSettings);
            var apiCallsBackOffStrategy = new ExponentialBackOffStrategy() { Counter = 0, MaxRetry = _maxRetry };
            var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), apiCallsBackOffStrategy, _runtime, _maxRuntime);
            ObservationResponse reportResponse = cancellableRetry.Execute(() => observationRequest.FetchDataAsync<ObservationResponse>().Result, "ApiRequest.FetchDataAsync");

            using (StreamWriter output = new(localFile.FullName, append: true))
            {
                output.Write(reportResponse.RawJson);

                recordsRequested += int.Parse(reportResponse.Limit);

                notDone = recordsRequested < int.Parse(reportResponse.Count);
                if (notDone)
                {
                    output.Write(",");
                }
            }
            counter++;

            //there is a rate limit of 120 calls per minute; counter resets on the minute
            //if counter reaches threshold then we should pause until next minute when the counter resets
            if (reportResponse.Header.TryGetValues(RateLimitHeaderRemaining, out IEnumerable<string> rateLimitRemainingHeaderValues))
            {
                string rateLimitRemainingHeader = rateLimitRemainingHeaderValues.First();
                if (int.TryParse(rateLimitRemainingHeader, out int callsRemaining) && callsRemaining <= _callThreshold)
                {
                    var secondsRemaining = (60 - DateTime.UtcNow.Second);
                    _log(LogLevel.Debug, $"Series id {report.ReportSettings.SeriesId} request has approached the threshold of {_callThreshold} calls remaining, will make next call in {secondsRemaining} seconds when the rate limit resets");
                    Task.Delay(secondsRemaining * 1000).Wait();
                }
            }

            _log(LogLevel.Debug, $"{queueItem.FileGUID}-Successful data download for Series ID {report.ReportSettings.SeriesId}; Call Summary: callCounter:{counter};recordsRequested:{recordsRequested};" +
                $"record count={reportResponse.Count}, startdate={reportResponse.ObservationStart}, enddate={reportResponse.ObservationEnd}; delaying {_millisecondsDelay}ms until next call");

            // delay between requests
            Task.Delay(_millisecondsDelay).Wait();
        } while (notDone);
    }

    private bool IsSchedule(APIReport<ObservationParameters> report)
    {
        var importDate = DateTime.UtcNow;

        // for daily jobs, we do not want to pull same data more than once a day so we check the last time the data was downloaded in a Lookup
        if (_seriesState.All(s => !s.SeriesId.Equals(report.ReportSettings.SeriesId, StringComparison.InvariantCultureIgnoreCase)))
        {
            _seriesState.Add(new SeriesState { SeriesId = report.ReportSettings.SeriesId });
        }
        _currentSeriesState = _seriesState.First(s => s.SeriesId.Equals(report.ReportSettings.SeriesId, StringComparison.InvariantCultureIgnoreCase));

        // if the LookUp date for the series matches today's date then the report has already been downloaded, so we do not want to schedule again
        var seriesDeltaDate = _currentSeriesState.DeltaDate == null ? DateTime.UtcNow.AddDays(-2) : _currentSeriesState.DeltaDate.Value.Date;
        if (importDate.Date == seriesDeltaDate.Date) return false;

        var isScheduled = false;
        var dateExpression = GenerateReportDateExpression(importDate);
        var reportScheduleDetails = report?.ReportSettings?.ReportScheduleDetails;
        if (reportScheduleDetails == null) return false;

        foreach (var reportSchedule in reportScheduleDetails.OrderByDescending(t => t.Priority))
        {
            if (reportSchedule.Interval == ReportSettings.IntervalEnum.Daily)
            {
                isScheduled = true;
                break;
            }

            //the "triggerDayRegex" can be either "1" or "Monday" as in "schedule job to run on the 1st" or "schedule for Monday"
            var triggerDayRegex = reportSchedule.TriggerDayRegex;
            int day = 1;
            var triggerIsNumber = int.TryParse(reportSchedule.TriggerDayRegex, out day);
            if (triggerIsNumber)
            {
                triggerDayRegex = reportSchedule.TriggerDayRegex.PadLeft(2, '0');
            }

            var reportScheduleRegex = (reportSchedule.Interval == ReportSettings.IntervalEnum.LastDayOfTheMonth) ? $"{reportSchedule.Interval}" : $"{reportSchedule.Interval}{triggerDayRegex}";

            if (Regex.IsMatch(dateExpression, reportScheduleRegex, RegexOptions.IgnoreCase))
            {
                _log(LogLevel.Debug, $"Matching schedule found for series id {report.ReportSettings.SeriesId} - {reportScheduleRegex}");
                isScheduled = true;
                break;
            }
        }

        return isScheduled;
    }

    private static string GenerateReportDateExpression(DateTime date)
    {
        List<string> dateExpressionComponents = new()
        {
            //prepend the interval enum so it will match the full regex pattern (except for "LASTDAYOFTHEMONTH")
            //examples: "EveryMonday", "Every15", "FirstSaturday"
            $"{ReportSettings.IntervalEnum.Every}{date.DayOfWeek.ToString().ToUpper()}",
            $"{ReportSettings.IntervalEnum.Every}{date:dd}"
        };

        if (UtilsDate.IsFirstDayOfMonth(date, date.DayOfWeek))
        {
            dateExpressionComponents.Add($"{ReportSettings.IntervalEnum.First}{date.DayOfWeek.ToString().ToUpper()}");
        }

        if (UtilsDate.IsLastDayOfMonth(date, date.DayOfWeek))
        {
            dateExpressionComponents.Add($"{ReportSettings.IntervalEnum.Last}{date.DayOfWeek.ToString().ToUpper()}");
        }

        if (UtilsDate.IsLastDayOfMonth(date))
        {
            dateExpressionComponents.Add($"{ReportSettings.IntervalEnum.LastDayOfTheMonth}");
        }

        string dateExpression = string.Join(" ", dateExpressionComponents);
        return dateExpression;
    }

    private FileSystemFile CreateLocalFile(Queue queueItem, APIReport<ObservationParameters> report)
    {
        string[] paths =
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), $"{queueItem.FileGUID}_{report.ReportSettings.SeriesId}_{queueItem.FileDate:yyyy-MM-dd}.json"
        };
        Uri localFileUri = RemoteUri.CombineUri(_baseLocalImportUri, paths);

        var localFile = new FileSystemFile(localFileUri);
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
        }

        return localFile;
    }

    private List<T> GetReportData<T>(string[] paths, bool isLocal = false, Newtonsoft.Json.JsonSerializerSettings jsonSettings = null) where T : new()
    {
        var reportData = new List<T>();
        jsonSettings ??= new Newtonsoft.Json.JsonSerializerSettings()
        {
            MissingMemberHandling = MissingMemberHandling.Ignore,
            NullValueHandling = NullValueHandling.Ignore
        };

        var sourceUri = isLocal ? this._baseLocalImportUri : this._baseDestUri;
        var filePath = RemoteUri.CombineUri(sourceUri, paths);

        using (var sourceStream = isLocal ? File.OpenRead(filePath.AbsolutePath) : _remoteAccessClient.WithFile(filePath).Get())
        {
            using (var txtReader = new StreamReader(sourceStream))
            {
                //the json raw file is a list of api response objects
                //we wrap the raw data with the AllData class so that we can deserialize the json as an array
                var deserializedJson = JsonConvert.DeserializeObject<AllData<T>>($"{{'allData':[{txtReader.ReadToEnd()}]}}", jsonSettings);
                if (deserializedJson.allData.Count != 0) reportData.AddRange(deserializedJson.allData);
            }
        }

        return reportData;
    }

    private void WriteObjectToFile(JArray entity, string entityID, DateTime fileDate, string filename)
    {
        string[] paths = new string[]
        {
            entityID.ToLower(), GetDatedPartition(fileDate), filename
        };

        IFile transformedFile = _remoteAccessClient.WithFile(RemoteUri.CombineUri(_baseStageDestUri, paths));
        ETLProvider.SerializeRedshiftJson(entity, transformedFile);
    }

    private void StageReport(Queue queueItem)
    {
        if (string.IsNullOrEmpty(queueItem.FileCollectionJSON))
        {
            _log(LogLevel.Debug, $"File Collection is empty; unable to stage data for FileGUID: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} ");
        }
        else
        {
            var reports = queueItem.FileCollection;
            Action<JArray, string, DateTime, string> writeToFileSignature = ((a, b, c, d) => WriteObjectToFile(a, b, c, d));

            foreach (var report in reports)
            {
                //locally saved files use the filedate in their filepath
                string[] paths = new string[]
                {
                    queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), report.FilePath
                };

                var fileName = $"{queueItem.FileGUID}_{report.SourceFileName}.json";

                _log(LogLevel.Debug, $"Staging Dimension Report for raw file: {report.FilePath}; report type {report.SourceFileName}; account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; fileGUID: {queueItem.FileGUID}");

                var observationData = GetReportData<ObservationResponse>(paths, true);
                FREDService.StageObservations(queueItem.EntityID, report.SourceFileName, queueItem.FileDate, observationData, fileName, writeToFileSignature);
            }
        }

        //archive all "raw" json files that were stored locally in a tar.gz file and upload to raw folder in s3
        ArchiveRawFiles(queueItem, queueItem.FileDate, "observation");

        _log(LogLevel.Debug, $"Changing queue status to complete. Job complete for account id: {queueItem.EntityID}; file date: {queueItem.FileDate}; file GUID: {queueItem.FileGUID}");
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update(queueItem);

        // update LookUp that tracks the last date we pulled data for each series ID
        var dbState = SetupService.GetById<Lookup>(Constants.FRED_SERIES_STATE);

        if (dbState != null)
        {
            var fredSeriesStateLookup = new Lookup
            {
                Name = Constants.FRED_SERIES_STATE,
                Value = JsonConvert.SerializeObject(_seriesState)
            };
            SetupService.Update(fredSeriesStateLookup);
        }
        else
        {
            SetupService.InsertIntoLookup(Constants.FRED_SERIES_STATE, JsonConvert.SerializeObject(_seriesState));
        }
    }

    private void ArchiveRawFiles(Queue queueItem, DateTime importDateTime, string fileType)
    {
        _log(LogLevel.Info, "Start archiving raw json data");

        var entityID = queueItem.EntityID.ToLower();
        Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, entityID);
        var sourceDirectory = tempLocalImportUri.AbsolutePath;

        string[] destPaths = new string[]
        {
            entityID, $"{importDateTime:yyyy-MM-dd}_{entityID}_{fileType}.tar.gz"
        };
        var archivePath = RemoteUri.CombineUri(_baseLocalImportUri, destPaths);
        var archiveFile = new FileSystemFile(archivePath);

        using (var stream = archiveFile.Create())
        {
            using (var gz = new GZipOutputStream(stream))
            {
                gz.SetLevel(3);
                using (var tar = new TarOutputStream(gz, Encoding.UTF8))
                {
                    var directories = Directory.GetDirectories(sourceDirectory);
                    foreach (string directory in directories)
                    {
                        var pathToCurrentDirectory = Path.Combine(sourceDirectory, directory);
                        string[] filenames = Directory.GetFiles(pathToCurrentDirectory);

                        foreach (string filename in filenames)
                        {
                            using (FileStream inputStream = File.OpenRead(filename))
                            {
                                string tarName = filename.Substring(3); // strip off "C:\"
                                long fileSize = inputStream.Length;
                                TarEntry entry = TarEntry.CreateTarEntry(tarName);
                                entry.Size = fileSize;
                                tar.PutNextEntry(entry);

                                byte[] localBuffer = new byte[32 * 1024];
                                while (true)
                                {
                                    int numRead = inputStream.Read(localBuffer, 0, localBuffer.Length);
                                    if (numRead <= 0)
                                        break;
                                    tar.Write(localBuffer, 0, numRead);
                                }
                            }
                            tar.CloseEntry();
                        }
                    }
                    tar.Close();
                }
                gz.Close();
            }
            stream.Close();
        }

        string[] s3paths = new string[]
        {
              entityID, GetDatedPartition(importDateTime), $"{importDateTime:yyyy-MM-dd}_{entityID}_{fileType}.tar.gz"
        };
        S3File s3archiveFile = new(RemoteUri.CombineUri(this._baseDestUri, s3paths), GreenhouseS3Creds);
        base.UploadToS3(archiveFile, s3archiveFile, s3paths);
        CleanupLocalEntityFolder(queueItem);
    }

    private void CleanupLocalEntityFolder(Queue queueItem)
    {
        Uri tempLocalImportUri = RemoteUri.CombineUri(_baseLocalImportUri, queueItem.EntityID.ToLower());
        FileSystemDirectory localImportDirectory = new FileSystemDirectory(tempLocalImportUri);
        if (localImportDirectory.Exists)
        {
            localImportDirectory.Delete(true);
        }
    }

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

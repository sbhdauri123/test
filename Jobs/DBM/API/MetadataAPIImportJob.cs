using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.DBM.API;
using Greenhouse.Data.DataSource.DBM.API.Resource;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Ordered;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.DBM.API;

[Export("DV360-ERFImportJob", typeof(IDragoJob))]
public class MetadataImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private const string API_REPORTNAME_ADVERTISER = "Advertisers";

    private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();

    private Uri _baseDestUri;
    private IOrderedEnumerable<OrderedQueue> _queueItems;

    private IEnumerable<Greenhouse.Data.Model.Aggregate.APIReport<ReportSettings>> _apiReports;
    private Auth.OAuthAuthenticator _oAuth;
    private Action<string> _logInfo;
    private Action<string> _logWarn;
    private Action<string, Exception> _logErrorExc;
    private int _maxPollyRetry;
    private int _s3PauseGetLength;
    private List<string> _advertiserIDs = new();
    private readonly Stopwatch _runtime = new Stopwatch();
    private TimeSpan _maxRuntime;
    private Dictionary<string, string> _lastRunDates = new Dictionary<string, string>();
    private BackOffStrategy _downloadPolicy;
    private int _exceptionCounter;
    private List<FileCollectionItem> _allCreatedFiles;
    private string _lastRunDateLookupValue;
    private readonly HashSet<string> _badEntities = new HashSet<string>();

    public void PreExecute()
    {
        base.Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
        this._oAuth = base.OAuthAuthenticator();
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID);
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _maxPollyRetry = string.IsNullOrEmpty(SetupService.GetById<Lookup>(Constants.DBM_METADATA_POLLY_MAX_RETRY)?.Value) ? 10 : int.Parse(SetupService.GetById<Lookup>(Constants.DBM_METADATA_POLLY_MAX_RETRY)?.Value);
        //pause in ms before getting the size of a file on S3
        //without that pause S3 randomly returns wrong values
        _s3PauseGetLength = int.Parse(SetupService.GetById<Lookup>(Constants.S3_PAUSE_GETLENGTH).Value);
        _lastRunDateLookupValue = $"{Constants.DBM_METADATA_LAST_DATE_RAN}_{CurrentIntegration.IntegrationID}";
        var LookupResult = SetupService.GetById<Lookup>(_lastRunDateLookupValue);
        _lastRunDates = string.IsNullOrEmpty(LookupResult?.Value) ? new Dictionary<string, string>() : JsonConvert.DeserializeObject<Dictionary<string, string>>(LookupResult.Value);

        if (!TimeSpan.TryParse(SetupService.GetById<Lookup>(Constants.DBM_METADATA_MAX_RUNTIME)?.Value, out _maxRuntime))
        {
            _maxRuntime = new TimeSpan(0, 3, 0, 0);
        }

        _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));
        _logWarn = (msg) => _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(msg)));
        _logErrorExc = (msg, exc) => _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(msg), exc));
    }

    public void Execute()
    {
        _runtime.Start();
        _logInfo($"EXECUTE START {base.DefaultJobCacheKey}");
        if (!_queueItems.Any())
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid("There are no reports in the Queue")));
            return;
        }

        foreach (var queueItem in _queueItems)
        {
            var reportList = new List<MetadataApiReportItem>();

            _advertiserIDs = new List<string>();
            // if this entity failed previously during this import, we move to the next queue
            if (_badEntities.Contains(queueItem.EntityID))
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{queueItem.FileGUID} EntityID: {queueItem.EntityID} for Entity ID: {queueItem.EntityID} failed previously, skipping queue.")));
                continue;
            }

            try
            {
                _allCreatedFiles = new List<FileCollectionItem>();

                //Getting last run dates for every entity - if our current filedate <= to what is in lookup
                //We've already ran thea queue and we log & skip the job
                //Otherwise run as normal & update lookup value with the latest date
                if (_lastRunDates.TryGetValue(queueItem.EntityID, out var lastDateRan) && queueItem.FileDate.Date <= DateTime.Parse(lastDateRan).Date)
                {
                    _logInfo("{queueItem.FileGUID} for Entity ID: {queueItem.EntityID} Already has up to date Metadata. Marking job as processing complete & skipping");

                    base.UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
                    continue;
                }

                //This is needed because _lastRunDate is updated only on successful queues
                if (_badEntities.Contains(queueItem.EntityID))
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{queueItem.FileGUID} EntityID: {queueItem.EntityID} for Entity ID: {queueItem.EntityID} has no Advertiser IDs in it, skipping queue.")));
                    continue;
                }

                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);
                foreach (var apiReport in _apiReports)
                {
                    var reportItem = new MetadataApiReportItem
                    {
                        ReportName = apiReport.APIReportName.ToLower(),
                        QueueID = queueItem.ID,
                        FileGuid = queueItem.FileGUID,
                        APIReport = apiReport,
                        FileDate = queueItem.FileDate,
                        ProfileID = queueItem.EntityID,
                        FileExtension = apiReport.ReportSettings?.FileExtension
                    };

                    reportList.Add(reportItem);
                }

                //We need the AdvertisersReport in order to query most of the other reports 
                DownloadAdvertiserReport(reportList, queueItem);

                DownloadReports(reportList, queueItem);

                //Updating last run date for current Queue
                _lastRunDates[queueItem.EntityID] = queueItem.FileDate.ToString();
                var LastRunDateLookup = new Lookup
                {
                    Name = _lastRunDateLookupValue,
                    Value = JsonConvert.SerializeObject(_lastRunDates)
                };
                Data.Repositories.LookupRepository repo = new Data.Repositories.LookupRepository();
                Data.Repositories.LookupRepository.AddOrUpdateLookup(LastRunDateLookup);
            }
            catch (HttpClientProviderRequestException hex)
            {
                SetQueueError(queueItem);
                _logErrorExc(
                    $"Exception - failed on queueID: {queueItem.ID}->FileGUID: {queueItem.FileGUID}->" +
                    $"for EntityID: {queueItem.EntityID} -> Exception details : {hex}", hex);
            }
            catch (WebException wex)
            {
                SetQueueError(queueItem);
                LogWebException(queueItem, wex);
            }
            catch (Exception exc)
            {
                SetQueueError(queueItem);
                _logErrorExc(
                    $"Exception - failed on queueID: {queueItem.ID}->FileGUID: {queueItem.FileGUID}->" +
                    $"for EntityID: {queueItem.EntityID} -> Exception: {exc.Message} -> StackTrace: {exc.StackTrace}", exc);
            }
        }

        _runtime.Stop();

        if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
        {
            _logInfo($"Runtime exceeded time allotted - {_runtime.ElapsedMilliseconds}ms");
            JobLogger.JobLog.Status = Constants.JobLogStatus.Complete.ToString();
            JobLogger.JobLog.Message = "Job RunTime exceeded max runtime.";
        }

        if (_badEntities.Count != 0)
        {
            _logWarn($"Not able to retrieve the data for the following entities - {string.Join(",", _badEntities)}");
            JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
            JobLogger.JobLog.Message = "Not able to retrieve some of the entities. Check Splunk for more info.";
        }

        if (_exceptionCounter > 0)
        {
            throw new ErrorsFoundException($"Total errors: {_exceptionCounter}; Please check Splunk for more detail.");
        }
        _logInfo("Import job complete");
    }

    private void DownloadAdvertiserReport(List<MetadataApiReportItem> adReports, OrderedQueue queueItem)
    {
        var adReport = adReports.First(x => x.APIReport.APIReportName == API_REPORTNAME_ADVERTISER);

        _downloadPolicy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxPollyRetry,
            Seed = 1,
            RandomMin = 0,
            RandomMax = 0
        };

        var getDownloadPolicy = new CancellableRetry(queueItem.FileGUID.ToString(), _downloadPolicy, _runtime, this._maxRuntime);

        getDownloadPolicy.Execute(() =>
        {
            DownloadReport(adReport, queueItem, null);
            if (_advertiserIDs.Count == 0)
            {
                throw new APIReportException($"Queue has no advertisers -> ID : {queueItem.ID} fileGuid: {queueItem.FileGUID} Entity ID: {queueItem.EntityID}");
            }
        });
    }

    private void DownloadReports(List<MetadataApiReportItem> reportList, OrderedQueue queueItem)
    {
        if (reportList.Count == 0) return;

        //We already pulled advertisers report, so not necessarily to pull now
        foreach (var reportItem in reportList.Where(report => report.APIReport.APIReportName != API_REPORTNAME_ADVERTISER))
        {
            _logInfo($"Downloading reports queueID: {queueItem.ID} " +
                              $"for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} Report:{reportItem.ReportName} ->");

            _downloadPolicy = new BackOffStrategy
            {
                Counter = 0,
                MaxRetry = _maxPollyRetry,
                Seed = 1,
                RandomMin = 0,
                RandomMax = 0
            };

            var getDownloadPolicy = new CancellableRetry(queueItem.FileGUID.ToString(), _downloadPolicy, _runtime, this._maxRuntime);

            switch (reportItem.APIReport.ReportSettings.EndpointID)
            {
                case EndpointID.AdvertiserID:
                    {
                        getDownloadPolicy.Execute(() =>
                        {
                            foreach (var advertiser in _advertiserIDs)
                            {
                                DownloadReport(reportItem, queueItem, advertiser);
                            }
                        });
                        break;
                    }
                default:
                    {
                        getDownloadPolicy.Execute(() =>
                        {
                            DownloadReport(reportItem, queueItem, null);
                        });
                        break;
                    }
            }
        }

        queueItem.FileSize += _allCreatedFiles.Sum(f => f.FileSize);
        queueItem.DeliveryFileDate = reportList.Max(x => x.FileDate);
        queueItem.Status = Constants.JobStatus.Complete.ToString();
        queueItem.StatusId = (int)Constants.JobStatus.Complete;
        JobService.Update((Queue)queueItem);
    }

    private void SetQueueError(Queue queue)
    {
        _badEntities.Add(queue.EntityID);
        _exceptionCounter++;
        queue.Status = Constants.JobStatus.Error.ToString();
        queue.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queue.ID, Constants.JobStatus.Error);
    }

    private void LogWebException(Queue queue, WebException wex)
    {
        HttpWebResponse httpWebResponse = wex?.Response as HttpWebResponse;

        if (httpWebResponse != null)
        {
            string errorMessage = string.Empty;

            using (StreamReader streamReader = new StreamReader(httpWebResponse.GetResponseStream()))
            {
                errorMessage = streamReader.ReadToEnd();
            }

            _logErrorExc(
                $"Web Exception - failed on queueID: {queue.ID}->FileGUID: {queue.FileGUID}->" +
                $"for EntityID: {queue.EntityID} -> Error Message: {errorMessage} -> Exception: {wex.Message} -> StackTrace: {wex.StackTrace}", wex);
        }
        else
        {
            _logErrorExc(
                $"Exception Error Running report status- failed on queueID: {queue.ID}->FileGUID: {queue.FileGUID}->" +
                $"for EntityID: {queue.EntityID} -> Exception: {wex.Message} -> StackTrace: {wex.StackTrace}", wex);
        }
    }
    /// <summary>
    /// returns true if report was downloaded successfully. Updates Queue item to error on exception.
    /// </summary>
    /// <param name="exceptionCount"></param>
    /// <param name="reportItem"></param>
    /// <param name="queueToDownload"></param>
    /// <param name="advertiserId"></param>
    /// <returns>Returns true if report was downloaded successfully. False otherwise</returns>
    private void DownloadReport(MetadataApiReportItem reportItem, OrderedQueue queueToDownload, string advertiserId)
    {
        string endpoint;

        var basePath = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/";
        switch (reportItem.APIReport.ReportSettings.EndpointID)
        {
            case EndpointID.AdvertiserID:
                {
                    endpoint = $"{basePath}{reportItem.APIReport.ReportSettings.Path}{advertiserId}/" + $"{(reportItem.APIReport.ReportSettings.EndPath ?? string.Empty)}";
                    break;
                }
            case EndpointID.PartnerID:
                {
                    endpoint = $"{basePath}{reportItem.APIReport.ReportSettings.Path}{queueToDownload.EntityID}" + $"{(reportItem.APIReport.ReportSettings.EndPath ?? string.Empty)}";
                    break;
                }
            default:
                {
                    endpoint = $"{basePath}{reportItem.APIReport.ReportSettings.Path}";
                    break;
                }
        }
        _logInfo($"Querying endpoint: {endpoint} to download reports");
        GetAllPaginatedData(endpoint, advertiserId, queueToDownload, reportItem);
    }

    private void GetAllPaginatedData(string endpoint, string advertiserId, IFileItem queueToDownload, MetadataApiReportItem reportItem)
    {
        string nextPageToken = "";
        int currentPage = 1;

        //the advertiserID is necessary for subsequent reports 
        do
        {
            string url = endpoint.TrimEnd('/');

            if (!string.IsNullOrEmpty(nextPageToken))
            {
                url = $"{url}&pageToken={nextPageToken}";
            }

            GetPaginatedData(url, advertiserId, reportItem, queueToDownload, currentPage, out nextPageToken);

            currentPage++;
            if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1) break;
        } while (!string.IsNullOrEmpty(nextPageToken));
    }

    private void GetPaginatedData(string url, string advertiserId, MetadataApiReportItem reportItem, IFileItem queueToDownload, int currentPage, out string nextPageToken)
    {
        string fileName;

        string responseJson = HttpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = url,
            Method = HttpMethod.Get,
            AuthToken = _oAuth.GetAccessToken,
            ContentType = MediaTypeNames.Application.Json
        }).GetAwaiter().GetResult();

        if (reportItem.APIReport.APIReportName == API_REPORTNAME_ADVERTISER && _advertiserIDs.Count == 0)
        {
            var deserializedResponse = JsonConvert.DeserializeObject<AdvertiserResponse>(responseJson);
            if (deserializedResponse.Advertisers == null || deserializedResponse.Advertisers.Count == 0)
            {
                nextPageToken = null;
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid($"QueueID:{queueToDownload.ID} Queue fileGuid: {queueToDownload.FileGUID} EntityID: {queueToDownload.EntityID} has no advertisers, skipping queue.")));
                return;
            }
            List<string> advertiserIds = deserializedResponse.Advertisers.ConvertAll(a => a.AdvertiserId);

            _advertiserIDs.AddRange(advertiserIds);
        }

        if (!string.IsNullOrEmpty(advertiserId) && url.Contains(advertiserId))
            fileName = $"{queueToDownload.FileGUID}_{reportItem.ReportName}_{reportItem.ProfileID}_{advertiserId}_page{currentPage}.{reportItem.FileExtension}";
        else
            fileName = $"{queueToDownload.FileGUID}_{reportItem.ReportName}_{reportItem.ProfileID}_page{currentPage}.{reportItem.FileExtension}";

        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{CurrentSource.SourceName} start DownloadReport: queueID: {queueToDownload.ID}->" +
            $"{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));

        string[] paths = new string[]
        {
                queueToDownload.EntityID.ToLower(), GetDatedPartition(queueToDownload.FileDate), fileName
        };

        // Upload each report to S3 individually
        S3File rawFile = new S3File(RemoteUri.CombineUri(this._baseDestUri, paths), GreenhouseS3Creds);
        var incomingFile = new StreamFile(new MemoryStream(Encoding.UTF8.GetBytes(responseJson)), GreenhouseS3Creds);
        //delete all files with same fileguid on S3
        base.UploadToS3(incomingFile, rawFile, paths, responseJson.Length);

        /*add to filelogcollection, calculate cumulative size, in filelogcollection populate filepath & filesize */
        var fileItem = new FileCollectionItem()
        {
            FileSize = rawFile.Length,
            SourceFileName = Path.GetFileNameWithoutExtension(fileName),
            FilePath = fileName
        };
        this._allCreatedFiles.Add(fileItem);

        //from time to time S3 will return the wrong file size
        //pausing has proven to reduce the probability of this issue happening
        Task.Delay(_s3PauseGetLength).Wait();
        reportItem.FileSize = rawFile.Length;

        if (rawFile.Length != responseJson.Length)
        {
            //When s3 file size does not match expected file size from GCS then retry the download 
            //by marking the report-item as not ready and exit here
            _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid($"Response length of {responseJson.Length} is not equal to s3 file size of {rawFile.Length}" +
                $": FileGUID: {queueToDownload.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}")));
            reportItem.IsReady = false;
        }

        reportItem.FileCollection = fileItem;

        reportItem.DeliveryFileDate = rawFile.LastWriteTimeUtc;
        // Check if there is a nextPageToken in the response
        // Parse JSOn, extract nextToken
        JObject responseObject = JObject.Parse(responseJson);
        string token = (string)responseObject["nextPageToken"];
        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"{CurrentSource.SourceName} end DownloadReport: FileGUID: {queueToDownload.FileGUID}->{reportItem.ReportID}->{reportItem.ReportName}->{reportItem.ReportURL}. Saving to S3 as {fileName}")));

        nextPageToken = token;
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

    ~MetadataImportJob()
    {
        Dispose(false);
    }
}

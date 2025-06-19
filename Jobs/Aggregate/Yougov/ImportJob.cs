using Amazon.S3.Transfer;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.NetBase;
using Greenhouse.Data.DataSource.YouGov;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using Polly;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.YouGov;

[Export("YouGov-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private RemoteAccessClient _remoteAccessClient;
    private Uri _baseDestUri;
    private Uri _baseStageDestUri;
    private List<IFileItem> _queueItems;
    private IEnumerable<APIReport<ReportSettings>> _apiReports;
    private LookupRepository _lookupRepo;
    private Action<string> _logInfo;
    private Action<string> _logDebug;
    private Action<string> _logError;
    private Action<string, Exception> _logErrorExc;
    private DateTime _lastDimCall;
    private Uri _baseLocalImportUri;
    private List<UsageEntry> _apiUsage;
    private int _maxNbCallPerminute;
    private int _maxSizePerMinuteInMB;
    private Dictionary<int, string> _regions;
    private IHttpClientProvider _httpClientProvider;

    private string JobGUID => base.JED.JobGUID.ToString();

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        _baseDestUri = GetDestinationFolder();
        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
        int nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);
        _queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, nbTopResult, this.JobLogger.JobLog.JobLogID)?.ToList();
        _remoteAccessClient = base.GetS3RemoteAccessClient();
        _apiReports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);
        _baseStageDestUri = new Uri(_baseDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
        _baseLocalImportUri = GetLocalImportDestinationFolder();
        _lookupRepo = new Data.Repositories.LookupRepository();
        var lastDimCall = SetupService.GetById<Lookup>(Common.Constants.YOUGOV_POLLY_DIM_DATE)?.Value;
        bool result = DateTime.TryParse(lastDimCall, out _lastDimCall);
        if (!result)
            _lastDimCall = default;

        _maxSizePerMinuteInMB = int.Parse(SetupService.GetById<Lookup>(Common.Constants.YOUGOV_USAGE_LIMIT_SIZE_MB)?.Value);

        _maxNbCallPerminute = int.Parse(SetupService.GetById<Lookup>(Common.Constants.YOUGOV_USAGE_LIMIT_NB)?.Value);

        _apiUsage = new List<UsageEntry>();

        _regions = new Dictionary<int, string>
        {
             {1, "Northeast"},
             {2, "South"},
             {3, "West"},
             {4, "Midwest"},
             {0, "National" }
        };

        _logInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        _logDebug = (msg) => logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid(msg)));
        _logError = (msg) => logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg)));
        _logErrorExc = (msg, exc) => logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg), exc));
    }

    public void Execute()
    {
        List<System.Tuple<System.Guid, Exception>> exceptions = new List<System.Tuple<System.Guid, Exception>>();
        if (!int.TryParse(SetupService.GetById<Lookup>(Constants.YOUGOV_POLLY_MAX_RETRY).Value, out int maxRetry))
            maxRetry = 10;

        var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
        {
            Counter = 3,
            MaxRetry = maxRetry
        };

        if (_queueItems.Count != 0)
        {
            var currentDateTime = DateTime.Now;

            var cookie = GetAuthorizationCookie(apiCallsBackOffStrategy);

            var dimReports = _apiReports.Where(r => r.ReportSettings.ReportType == "dim");
            var factReports = _apiReports.Where(r => r.ReportSettings.ReportType == "fact");

            foreach (Queue queue in _queueItems)
            {
                try
                {
                    _logInfo($"Getting report for queue ID={queue.ID} FileGUID={queue.FileGUID}");
                    var fileCollection = GetReports(
                                                                                    queue,
                                                                                    apiCallsBackOffStrategy,
                                                                                    cookie,
                                                                                    dimReports,
                                                                                    factReports,
                                                                                    currentDateTime);

                    queue.FileCollectionJSON = JsonConvert.SerializeObject(fileCollection);
                    queue.FileSize = fileCollection.Sum(f => f.FileSize);
                    queue.Status = Constants.JobStatus.Complete.ToString();
                    queue.StatusId = (int)Constants.JobStatus.Complete;
                    JobService.Update(queue);
                }
                catch (HttpClientProviderRequestException exc)
                {
                    HandleException(exceptions, queue, exc);
                }
                catch (Exception exc)
                {
                    HandleException(exceptions, queue, exc);
                }
            }

            if (exceptions.Count > 0)
            {
                throw new ErrorsFoundException($"Total errors: {exceptions.Count}; Please check Splunk for more detail.");
            }
        }
        else
        {
            _logInfo("There are no reports in the Queue");
        }

        _logInfo($"EXECUTE END {base.DefaultJobCacheKey}");
    }

    private void HandleException<TException>(List<Tuple<Guid, Exception>> exceptions, Queue queue, TException ex) where TException : Exception
    {
        exceptions.Add(System.Tuple.Create<System.Guid, Exception>(queue.FileGUID, ex));
        queue.Status = Constants.JobStatus.Error.ToString();
        queue.StatusId = (int)Constants.JobStatus.Error;
        JobService.UpdateQueueStatus(queue.ID, Constants.JobStatus.Error);

        var logMessage = BuildLogErrorMessage(queue, ex);
        _logErrorExc(logMessage, ex);
    }

    private static string BuildLogErrorMessage<TException>(Queue queue, TException exception) where TException : Exception
    {
        return exception switch
        {
            HttpClientProviderRequestException httpEx => $"Error with queue item -> failed on: {queue.FileGUID} for EntityID: {queue.EntityID} " +
                $"FileDate: {queue.FileDate} -> Exception details : {httpEx}",
            _ => $"Error with queue item -> failed on: {queue.FileGUID} for EntityID: {queue.EntityID} " +
                $"FileDate: {queue.FileDate} -> Exception: {exception.Message} - STACK {exception.StackTrace}"
        };
    }
    private sealed class UsageEntry
    {
        public DateTime ResponseTime { get; set; }
        public int ResponseSize { get; set; }
    }

    private List<FileCollectionItem> GetReports(Queue queue,
        IBackOffStrategy apiCallsBackOffStrategy,
        string cookie,
        IEnumerable<APIReport<ReportSettings>> dimReports,
        IEnumerable<APIReport<ReportSettings>> factReports,
        DateTime currentDateTime
    )
    {
        string currentApiEntityID = queue.EntityID.ToLower();
        //create local file to store raw json
        string[] paths =
        {
                    currentApiEntityID, GetDatedPartition(queue.FileDate)
                };

        var fileCollection = new List<FileCollectionItem>();
        JobService.UpdateQueueStatus(queue.ID, Constants.JobStatus.Running);

        _logInfo($"Cleaning up local folder for queue ID={queue.ID} FileGUID={queue.FileGUID}");
        CleanupLocalEntityFolder(queue);
        var getPolicy = GetPollyPolicy<Exception>(queue.FileGUID.ToString(), apiCallsBackOffStrategy);

        var dimFiles = GetDimReports(queue, cookie, dimReports, currentDateTime, getPolicy, paths, currentApiEntityID);
        fileCollection.AddRange(dimFiles);

        var factFiles = GetFactReports(queue, cookie, factReports, getPolicy, paths, currentApiEntityID);
        fileCollection.AddRange(factFiles);

        return fileCollection;
    }

    private List<FileCollectionItem> GetFactReports(Queue queue, string cookie, IEnumerable<APIReport<ReportSettings>> factReports,
        Policy getPolicy, string[] paths, string currentApiEntityID)
    {
        dynamic date = JsonConvert.DeserializeObject($"{{\"date\":\"{queue.FileDate.ToString("yyyy-MM-dd")}\"}}");
        var fileCollection = new List<FileCollectionItem>();
        foreach (var report in factReports)
        {
            _logInfo($"Getting fact report {report.APIReportName}");

            foreach (int regionId in _regions.Keys)
            {
                _logInfo($"Getting fact report {report.APIReportName} - RegionId: {regionId} RegionName: {_regions[regionId]}");

                string filter = regionId != 0 ? report.ReportSettings.Filters.Replace("@RegionID", regionId.ToString()) : "[]";


                var reportRequest = new ReportRequest
                {
                    Data = new ReportRequestData
                    {
                        ID = report.APIReportName + "_" + queue.FileGUID.ToString(),
                        Title = "Report Generated on " + DateTime.Now.ToLongDateString(),
                        Queries = new List<Query>
                        {
                            new Query
                            {
                                ID = report.APIReportName + "_" + queue.FileGUID.ToString(),
                                Entity = new Entity
                                {
                                    Region = "us",
                                    BrandsFromSectorID = int.Parse(currentApiEntityID)
                                },
                                Filters = JsonConvert.DeserializeObject(filter),
                                MetricsScoreTypes =
                                    JsonConvert.DeserializeObject(report.ReportSettings.MetricsScoreTypes),
                                Period = new Period
                                {
                                    StartDate = date,
                                    EndDate = date
                                },
                                Scoring = "total"
                            }
                        }
                    },
                    Meta = new ReportRequestMeta
                    {
                        Version = report.ReportSettings.Version
                    }
                };
                string bodyReport = JsonConvert.SerializeObject(reportRequest);
                string factReport = getPolicy.Execute(
                    _ => _httpClientProvider.SendRequestAsync(new HttpRequestOptions
                    {
                        Uri = "https://api.brandindex.com/v1/analyses/execute",
                        Method = HttpMethod.Post,
                        ContentType = MediaTypeNames.Application.Json,
                        Content = new StringContent(bodyReport, Encoding.UTF8, MediaTypeNames.Application.Json),
                    }).GetAwaiter().GetResult(),//make same as in the end
                new Dictionary<string, object> { { "methodName", "GetFactReports" } });
                LogUsage(factReport);
                string fileName = $"{queue.FileGUID}_{report.APIReportName}_{regionId}.json";
                var localFile = CreateLocalFile(paths, fileName);

                using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
                {
                    output.Write(factReport);
                }

                var fullPath = paths.Concat(new string[1] { fileName }).ToArray();
                UploadFileToS3(fullPath, localFile);

                fileCollection.Add(new FileCollectionItem
                {
                    FilePath = string.Join("/", paths),
                    SourceFileName = fileName,
                    FileSize = factReport.Length
                });

                var context = new Dictionary<string, string>
                {
                     {"region", _regions[regionId]}
                };

                StageFile(queue, report, factReport, currentApiEntityID, fileName, context);
            }
        }

        return fileCollection;
    }

    private void LogUsage(string body)
    {
        _apiUsage.Add(new UsageEntry
        {
            ResponseSize = body.Length,
            ResponseTime = DateTime.Now
        });

        // if we can only make 200 calls per minute, we take the entry made 200 calls ago
        // and check the the time log is greater than 1 minute
        var entryLimitTime = _apiUsage.OrderByDescending(a => a.ResponseTime).Skip(_maxNbCallPerminute - 1)?.Take(1);
        if (entryLimitTime != null && entryLimitTime.Any())
        {
            TimeSpan ts = DateTime.Now - entryLimitTime.First().ResponseTime;
            if (ts.TotalSeconds < 60)
            {
                var seconds = (60 - (int)ts.TotalSeconds);
                _logInfo($"Nb call per minute reached - waiting {seconds} seconds");
                Task.Delay(seconds * 1000).Wait();
            }
        }

        // second test is on the response size
        // the limit is 10MB for 1 minute - we are retrieving all the usage up for the last 1 min
        // if below that limit we are good - otherwise determinate how long to wait
        var oneMinAgo = DateTime.Now.AddMinutes(-1);
        var entriesLastMinute = _apiUsage.Where(a => a.ResponseTime >= oneMinAgo);
        var totalReceivedLastMinute = entriesLastMinute.Sum(e => e.ResponseSize);
        long diffInBytes =
            (long)Math.Round(totalReceivedLastMinute - ((_maxSizePerMinuteInMB * 1024 * 1024) * 0.9));// keeping 90% of the max as we dont want to reach the max size and we dont know how much data will come from the next call
        if (diffInBytes >= 0)
        {
            //let s retrieve how many of the older calls we need to make up the difference
            //and then wait difference between 1 minute after that last call and now
            var ordered = entriesLastMinute.OrderBy(a => a.ResponseTime);
            long sum = 0;

            foreach (var usage in ordered)
            {
                sum += usage.ResponseSize;
                if (sum >= diffInBytes)
                {
                    TimeSpan ts = usage.ResponseTime.AddMinutes(1) - DateTime.Now;
                    if (ts.TotalSeconds > 0)
                    {
                        var seconds = (int)ts.TotalSeconds;
                        _logInfo($"response size limit reached - waiting {seconds} seconds");

                        Task.Delay(seconds * 1000).Wait();
                        break;
                    }
                }
            }
        }
    }

    private List<FileCollectionItem> GetDimReports(Queue queue, string cookie, IEnumerable<APIReport<ReportSettings>> dimReports, DateTime currentDateTime,
        Policy getPolicy, string[] paths, string currentApiEntityID)
    {
        var fileCollection = new List<FileCollectionItem>();
        bool saveDate = false;
        string APIEntityTag = "@APIEntityCode";

        foreach (var report in dimReports)
        {
            string dimReport = @"{""meta"": {},""data"": {}}";

            if (_lastDimCall != default && _lastDimCall.Date < currentDateTime.Date)
            {
                _logInfo($"Retrieving dim report {report.APIReportName}");
                string url = report.ReportSettings.URL.Replace(APIEntityTag, currentApiEntityID);

                dimReport = getPolicy.Execute(
                    _ => _httpClientProvider.SendRequestAsync(new HttpRequestOptions
                    {
                        Uri = url,
                        Method = HttpMethod.Get,
                        Headers = new Dictionary<string, string> { { "Cookie", cookie } }
                    }).GetAwaiter().GetResult(),
                    new Dictionary<string, object> { { "methodName", $"GetDim-{report.APIReportName}" } });
                LogUsage(dimReport);

                saveDate = true;
            }
            else
            {
                _logInfo($"dim report {report.APIReportName} already retrieved today - using empty file");
            }

            string fileName = $"{queue.FileGUID}_{report.APIReportName}.json";
            var localFile = CreateLocalFile(paths, fileName);

            using (StreamWriter output = new StreamWriter(localFile.FullName, false, Encoding.UTF8))
            {
                output.Write(dimReport);
            }

            var fullPath = paths.Concat(new string[1] { fileName }).ToArray();
            UploadFileToS3(fullPath, localFile);

            fileCollection.Add(new FileCollectionItem
            {
                FilePath = string.Join("/", paths),
                SourceFileName = fileName,
                FileSize = dimReport.Length
            });

            _logInfo($"Staging dim report {report.APIReportName}");
            StageFile(queue, report, dimReport, currentApiEntityID, fileName);
        }

        if (saveDate)
        {
            _logInfo($"Saving YOUGOV_POLLY_DIM_DATE to Lookup table");
            _lastDimCall = currentDateTime;
            LookupRepository.AddOrUpdateLookup(new Lookup
            {
                Name = Common.Constants.YOUGOV_POLLY_DIM_DATE,
                Value = _lastDimCall.ToString("MM/dd/yyyy")
            });
        }

        return fileCollection;
    }

    private void StageFile(Queue queue, APIReport<ReportSettings> report, string dimReport, string currentApiEntityID, string fileName, Dictionary<string, string> context = null)
    {
        //stage dim report
        switch (report.APIReportName)
        {
            case "BrandDim":
                _logInfo($"Staging {report.APIReportName}");
                var brandReportObject = JsonConvert.DeserializeObject<BrandDimResponse>(dimReport);
                YouGovService.StageBrandReport(currentApiEntityID, queue.FileDate, brandReportObject, fileName,
                    WriteObjectToFile);
                break;

            case "SectorDim":
                _logInfo($"Staging {report.APIReportName}");
                var sectorReportObject = JsonConvert.DeserializeObject<SectorDimResponse>(dimReport);
                YouGovService.StageSectorReport(currentApiEntityID, queue.FileDate, sectorReportObject, fileName,
                    WriteObjectToFile);
                break;

            case "NetScore":
                _logInfo($"Staging {report.APIReportName}");
                var metricsFactReportObject = JsonConvert.DeserializeObject<MetricsFactResponse>(dimReport);
                YouGovService.StageFactReport(currentApiEntityID, queue.FileDate, metricsFactReportObject, fileName,
                    WriteObjectToFile, context["region"]);
                break;
        }
    }

    private void UploadFileToS3(string[] paths, FileSystemFile localFile)
    {
        Uri destUri = RemoteUri.CombineUri(GetDestinationFolder(), paths);
        Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
        TransferUtility transferUtility = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
        transferUtility.UploadAsync(localFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
        _logInfo($"TransferUtility S3 URI {destUri} upload complete");
        localFile.Delete();
    }

    private FileSystemFile CreateLocalFile(string[] paths, string fileName)
    {
        var fullPath = paths.ToList();
        fullPath.Add(fileName);
        Uri localFileUri = RemoteUri.CombineUri(_baseLocalImportUri, fullPath.ToArray());

        var localFile = new FileSystemFile(localFileUri);
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
        }

        return localFile;
    }

    private string GetAuthorizationCookie(IBackOffStrategy apiCallsBackOffStrategy)
    {
        _logInfo($"Getting Authorization Cookie");

        var authenticationRequest = new AuthenticationRequest
        {
            Data = new Data.DataSource.YouGov.Data
            {
                Email = this.CurrentCredential.CredentialSet.email,
                Password = this.CurrentCredential.CredentialSet.password
            },
            Meta = new Meta
            {
                Version = "v1"
            }
        };

        string body = JsonConvert.SerializeObject(authenticationRequest);

        Policy getPolicy = GetPollyPolicy<Exception>("Call For Authorization Cookie", apiCallsBackOffStrategy);

        HttpResponseMessage responseAuthentication = getPolicy.Execute(
            _ => _httpClientProvider.GetResponseAsync(new HttpRequestOptions
            {
                Uri = CurrentIntegration.EndpointURI,
                Method = HttpMethod.Post,
                ContentType = MediaTypeNames.Application.Json,
                Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),
            }).GetAwaiter().GetResult(),
            new Dictionary<string, object> { { "methodName", "GetAuthorizationCookie" } });

        string cookie;
        if (responseAuthentication != null &&
            responseAuthentication.Headers.TryGetValues("Set-Cookie", out IEnumerable<string> values))
        {
            cookie = string.Join(", ", values);
        }
        else
        {
            throw new APIResponseException("Authentication call failed: cookie is null or empty");
        }

        return cookie;
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

    private void WriteObjectToFile(JArray entity, string entityID, DateTime fileDate, string filename)
    {
        string[] paths = new string[]
        {
            entityID.ToLower(), GetDatedPartition(fileDate), filename
        };

        IFile transformedFile = _remoteAccessClient.WithFile(RemoteUri.CombineUri(_baseStageDestUri, paths));
        ETLProvider.SerializeRedshiftJson(entity, transformedFile);
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

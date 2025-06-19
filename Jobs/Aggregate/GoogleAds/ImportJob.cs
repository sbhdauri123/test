using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.DataSource.GoogleAds;
using Greenhouse.Data.DataSource.GoogleAds.Aggregate;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Jobs.Infrastructure.Retry;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.GoogleAds;

[Export("GoogleAds-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private IEnumerable<APIReport<ReportSettings>> reports;
    private GoogleAdsClient googleAdsClient;

    private string JobGUID => base.JED.JobGUID.ToString();

    private string DeveloperToken => CurrentCredential.CredentialSet?.DeveloperToken?.ToString();
    private string MCC => CurrentCredential.CredentialSet?.MCC?.ToString();
    private LookupRepository lookupRepository;
    private string filters;
    private int pauseBetweenCalls;
    private int maxRetry;
    private bool hasException;
    private Action<string> LogInfo;
    private Action<string> LogWarning;
    private Action<string> LogError;
    private Action<string, Exception> LogErrorExc;
    private int nbTopResult;
    private string getCustomersQuery;
    private List<string> apiExceptionsToCancel;
    private List<string> knownInvalidCustomerIds = new List<string>();
    private readonly Stopwatch runtime = new Stopwatch();
    private TimeSpan maxRuntime;
    private string googleAdsReplaceStrings;
    private int? logLevelSDK;

    private int maxParallelImport;
    private readonly Object lockObject = new Object();
    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();
        LogInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        LogWarning = (msg) => logger.Log(Msg.Create(LogLevel.Warn, logger.Name, PrefixJobGuid(msg)));
        LogError = (msg) => logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg)));
        LogErrorExc = (msg, exc) => logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(msg), exc));

        LogInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        this.reports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        this.lookupRepository = new LookupRepository();
        bool enableDetailedLogs = InitFromLookUp();

        var oAuth = base.OAuthAuthenticator();

        this.googleAdsClient = new GoogleAdsClient(_httpClientProvider, DeveloperToken,
            MCC,
            oAuth,
            JobGUID,
            LogInfo,
            enableDetailedLogs,
            googleAdsReplaceStrings,
            logLevelSDK,
            lockObject);
    }

    private bool InitFromLookUp()
    {
        this.nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);

        this.filters = SetupService.GetById<Lookup>(Constants.GOOGLEADS_WHERE_FILTERS)
            .Value;

        this.pauseBetweenCalls = int.Parse(SetupService.GetById<Lookup>(Constants.GOOGLEADS_PAUSE_BETWEEN_CALLS).Value);

        this.getCustomersQuery = SetupService.GetById<Lookup>(Constants.GOOGLEADS_CUSTOMER_QUERY)
            ?.Value;

        this.maxRetry = LookupService.GetLookupValueWithDefault(Constants.GOOGLEADS_POLLY_MAX_RETRY, 10);

        // default max run time will be 3 hours if no lookup exists
        this.maxRuntime = LookupService.GetLookupValueWithDefault(Constants.GOOGLEADS_MAX_RUNTIME, new TimeSpan(0, 3, 0, 0));
        this.maxParallelImport = LookupService.GetLookupValueWithDefault(Constants.GOOGLEADS_MAX_PARALLEL_IMPORT, 1);

        Lookup lookupError =
            SetupService.GetById<Lookup>(Constants.GOOGLEADS_API_ERRORS);
        if (lookupError?.Value != null)
        {
            apiExceptionsToCancel = lookupError.Value.Split('|').ToList();
        }
        else
        {
            apiExceptionsToCancel = new List<string>();
        }

        Lookup lookupKnownCustomerIds =
            SetupService.GetById<Lookup>(Constants.GOOGLEADS_KNOWN_INVALID_CUSTOMERS);
        if (lookupKnownCustomerIds?.Value != null)
        {
            knownInvalidCustomerIds = lookupKnownCustomerIds.Value.Split(',').ToList();
        }

        // any characters matching regex pattern will be replaced with an empty string (eg new line - "\n")
        Lookup lookupGoogleAdsReplaceStrings = SetupService.GetById<Lookup>(Constants.GOOGLEADS_REPLACE_STRINGS);
        if (!string.IsNullOrEmpty(lookupGoogleAdsReplaceStrings?.Value))
        {
            googleAdsReplaceStrings = lookupGoogleAdsReplaceStrings.Value;
        }

        Lookup logLevelSDKLookup = SetupService.GetById<Lookup>(Constants.GOOGLEADS_SDK_LOG_LEVEL);
        if (!string.IsNullOrEmpty(logLevelSDKLookup?.Value) && int.TryParse(logLevelSDKLookup?.Value, out int logLevel))
        {
            logLevelSDK = logLevel;
        }
        else
        {
            logLevelSDK = null;
        }

        Lookup lookupEnableDetailedLogs =
            SetupService.GetById<Lookup>(Constants.GOOGLEADS_ENABLE_DETAILED_LOGS);
        bool enableDetailedLogs = bool.Parse(lookupEnableDetailedLogs.Value);
        return enableDetailedLogs;
    }

    public void Execute()
    {
        var queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, this.nbTopResult, this.JobLogger.JobLog.JobLogID, CurrentIntegration.IntegrationID)?.ToList();

        runtime.Start();

        if (queueItems.Count != 0)
        {
            var invalidCustomerIDs = DownloadAllReports(queueItems);

            if (this.hasException)
            {
                throw new ErrorsFoundException($"Errors have been logged; Please check Splunk for more detail.");
            }
            else if (invalidCustomerIDs.Count != 0)
            {
                var subList = UtilsText.GetSublistFromList(invalidCustomerIDs, 75);
                int count = 0;

                foreach (var list in subList)
                {
                    string message = $"Warning - Invalid CustomerIds ({count++}/{subList.Count()}): " + string.Join(",", list);
                    LogWarning(message);
                }

                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = "Invalid Customers! For full list search for 'Warning - Invalid CustomerIds' in splunk";
            }
        }//end if queue.Any()
        else
        {
            LogInfo("There are no queue to Import");
        }

        LogInfo("Import job complete");
    }

    private List<string> DownloadAllReports(List<IFileItem> queueItems)
    {
        /* foreach queue
             ->foreach report 
                -> get client reports
        */

        var queueByEntity = queueItems.GroupBy(grp => grp.EntityID);

        var customersNoAccess = new ConcurrentBag<string>();

        var options = new ParallelOptions { MaxDegreeOfParallelism = this.maxParallelImport };

        Parallel.ForEach(queueByEntity, options, queues =>
        {
            LogInfo($"Start Parallel.ForEach for EntityID '{queues.Key}'");
            var noAccess = DownloadReports(queues.ToList());
            noAccess.ForEach(f => customersNoAccess.Add(f));
            LogInfo($"End Parallel.ForEach for EntityID '{queues.Key}'");
        });

        return customersNoAccess.Distinct().ToList();
    }

    private List<string> DownloadReports(List<IFileItem> queues)
    {
        var customersNoAccess = new List<string>();
        bool staticReportsDownloaded = false;

        try
        {
            var customersForAccount = GetCustomersForAccount(queues.First());

            foreach (Queue queueItem in queues)
            {
                LogInfo($"Importing queue Fileguid= {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}");

                if (TimeSpan.Compare(runtime.Elapsed, maxRuntime) == 1)
                {
                    //the runtime is greater than the max RunTime
                    LogWarning($"Import job error ->  Exception: The runtime ({runtime.Elapsed}) exceeded the allotted time {maxRuntime}");
                    break;
                }

                //make sure FileCollection is always empty at start of Import job, to avoid duplicate files from being added
                queueItem.FileCollectionJSON = null;
                JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                var fileCollectionItems = new List<FileCollectionItem>(this.reports.Count());

                IFile localFile = null;

                // if a customer doesnt return any result for a report, there is no need to make
                // other calls for other reports
                var customersNoResult = new List<string>();

                PrepareLocalFolder(queueItem);

                var hasFailedReport = false;
                //sorting by Level, if a level 1 Campain doesnt have data, no need to request level 2 (AdGroup) or level 3 (AdGroup Ad)
                foreach (var report in this.reports.OrderBy(r => r.ReportSettings.Order))
                {
                    LogInfo($"Importing queue for report {report.APIReportName} ({report.APIReportID}) Fileguid= {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}");
                    var customers = new List<CustomerClients>();

                    // static reports (labels) only need to be downloaded once per entity per job
                    // because the google sql query to retrieve data does not have any segmentation date or filtering
                    if (report.ReportSettings.IsStatic && staticReportsDownloaded)
                    {
                        LogInfo(
                            $"Skipping Download for {report.APIReportName}-previously downloaded for Entity {queueItem.EntityID}");
                        continue;
                    }

                    // For some accounts like Disney, the queue.EntityID contains the customer account already. This is the lowest item in the tree, 
                    // no children to retrieve
                    if (report.ReportSettings.CallForEntityID)
                    {
                        var parentClient = new CustomerClients
                        { CustomerClient = new CustomerClient { Id = queueItem.EntityID } };
                        customers.Add(parentClient);
                    }
                    else
                    {
                        customers.AddRange(customersForAccount);
                    }

                    localFile = CreateLocalFile(queueItem, report);

                    var fileDetails =
                        DownloadReportForCustomers(report, localFile, queueItem, customers, customersNoResult, customersNoAccess);

                    if (fileDetails == null)
                    {
                        hasFailedReport = true;
                        break; //the report was not saved to drive, stop the loop, and once out of the loop move to the next queue
                    }

                    fileCollectionItems.Add(fileDetails);

                    LogInfo($"END - Importing queue Fileguid= {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}");
                } //foreach report

                // we dont have all the reports,
                // no further action for this queue, moving to the next one
                if (hasFailedReport)
                    continue;

                // we have all the reports on the local drive, let upload them to s3 and update the queue
                UploadToS3(localFile, queueItem);

                queueItem.Status = Constants.JobStatus.Complete.ToString();
                queueItem.StatusId = (int)Constants.JobStatus.Complete;
                queueItem.FileCollectionJSON = JsonConvert.SerializeObject(fileCollectionItems);
                JobService.Update((Queue)queueItem);

                localFile.Directory.Delete(true);

                // add entity ID to running list of Entity IDs that have downloaded static reports
                // to avoid repeated download of same data in this job instance
                if (reports.Any(r => r.ReportSettings.IsStatic && !staticReportsDownloaded))
                {
                    staticReportsDownloaded = true;
                }
            } //end foreach queue 
        }
        catch (HttpClientProviderRequestException exc)
        {
            HandleException(queues, exc);
        }
        catch (Exception exc)
        {
            HandleException(queues, exc);
        }

        return customersNoAccess;
    }

    private void HandleException<TException>(List<IFileItem> queues, TException exc) where TException : Exception
    {
        // rule: if an unexpected exception happens for an entity, we mark all queues with that entity as error and stop
        hasException = true;

        UpdateQueueStatus(queues, Constants.JobStatus.Error);

        var logMsg = BuildLogMessage(exc);
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, base.PrefixJobGuid(logMsg), exc));
    }
    private static string BuildLogMessage<TException>(TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx => $"Error -> Exception details : {httpEx}",
            _ => $"Error -> Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }
    private void UploadToS3(IFile localFile, Queue queueItem)
    {
        foreach (var file in localFile.Directory.GetFiles())
        {
            // upload to S3 raw
            string[] paths = new string[]
            {
                queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), file.Name
            };
            var destUri = GetUri(paths, Constants.ProcessingStage.RAW);
            var rawFile = new S3File(destUri, GreenhouseS3Creds);
            base.UploadToS3(file, rawFile, paths);
        }
    }

    private IFile CreateLocalFile(Queue queueItem, APIReport<ReportSettings> report)
    {
        IFile localFile;
        string fileName = GetFileName(queueItem, report);

        string[] paths =
        {
            queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), fileName
        };

        Uri tempDestUri = RemoteUri.CombineUri(base.GetLocalImportDestinationFolder(), paths);
        localFile = new FileSystemFile(tempDestUri);

        return localFile;
    }

    private void PrepareLocalFolder(Queue queue)
    {
        string[] paths =
        {
            queue.EntityID.ToLower(), GetDatedPartition(queue.FileDate), "nofile"
        };

        Uri localFolder =
            RemoteUri.CombineUri(base.GetLocalImportDestinationFolder(), paths);
        var folder = new FileSystemFile(localFolder);

        //if first report, let s start on a clean folder
        if (folder.Directory.Exists)
        {
            folder.Directory.Delete(true);
        }

        folder.Directory.Create();
    }

    public static string GetFileName(Queue queueItem, APIReport<ReportSettings> report)
    {
        return $"{queueItem.FileGUID}_{report.ReportSettings.ReportName}_{report.ReportSettings.ReportType}.{report.ReportSettings.FileExtension}";
    }

    private FileCollectionItem DownloadReportForCustomers(APIReport<ReportSettings> report, IFile localFile, Queue queue, List<CustomerClients> customers, List<string> customersNoResult, List<string> customersNoAccess)
    {
        using (StreamWriter fileStream = new StreamWriter(localFile.FullName, true, Encoding.UTF8))
        {
            //creating a header
            var requestedColumns = report.ReportFields.Where(r => r.IsActive)
                    .OrderBy(r => r.SortOrder)
                    .Select(r => r.APIReportFieldName);
            string header = string.Join(",", requestedColumns);
            fileStream.WriteLine(header.Replace('.', '_'));

            //For each customer client Get report data and concatenate to 1 report file

            try
            {
                string googleSql = BuildSql(report, queue);
                LogInfo($"Writing content of report Report Name :{report.APIReportName} Report Type: {report.ReportSettings.ReportType} For: FileGUID: {queue.FileGUID}. SQL Call: {googleSql}");

                foreach (var customer in customers)
                {
                    var customerID = customer.CustomerClient.Id;

                    if (this.knownInvalidCustomerIds.Contains(customerID))
                    {
                        LogInfo($"CustomerID {customerID} is known to be invalid (from Lookup) and will be skipped.");
                        continue;
                    }

                    var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
                    {
                        Counter = 3,
                        MaxRetry = this.maxRetry
                    };

                    var cancellableRetry = new CancellableRetry(queue.FileGUID.ToString(), apiCallsBackOffStrategy, this.apiExceptionsToCancel, this.runtime, this.maxRuntime);
                    DownloadReport(googleSql, fileStream, customerID, report, queue, cancellableRetry, customersNoResult, customersNoAccess);
                } //end foreach client
            }
            catch (Exception exc)
            {
                LogErrorExc(
                    $"Error: daily report -> failed on: {queue.FileGUID} for EntityID: {queue.EntityID} FileDate: {queue.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}"
                    , exc);
                fileStream.Close();
                localFile.Directory.Delete(true);
                throw;
            }

            fileStream.Close();
        }

        //Upload all files at once when all reports are done downloading
        FileCollectionItem fileItem = new FileCollectionItem()
        {
            FileSize = localFile.Length,
            SourceFileName = report.ReportSettings.ReportName,
            FilePath = localFile.Name
        };

        queue.FileSize += fileItem.FileSize;

        return fileItem;
    }

    private void DownloadReport(string googleSql,
        StreamWriter fileStream, string customerID, APIReport<ReportSettings> report, Queue queue,
        CancellableRetry cancellableRetry, List<string> customersWithNoResult, List<string> customersNoAccess)
    {
        Dictionary<string, string> contextInfo = new Dictionary<string, string>()
        {
            {"queue ID", queue.ID.ToString()},
            {"queue EntityID", queue.EntityID},
            {"Fileguid", queue.FileGUID.ToString()},
            {"APIReport ID/Name", $"{report.APIReportID}/{report.APIReportName}"}
        };

        // if a customer did not return any data for a previous call (of higher level)
        // no need to make another call
        if (customersNoAccess.Contains(customerID) || (customersWithNoResult.Contains(customerID) && !report.ReportSettings.CustomersNoResultOverride))
            return;

        bool hasError = false;

        int streamLines = cancellableRetry.Execute(
            () =>
            {
                return this.googleAdsClient.DownloadAsCSV(customerID, googleSql, fileStream, report.ReportFields, contextInfo, LogInfo);
            },
            "DownloadReport",
            (context) =>
            {
                // the call failed, checking if we got any known exception message
                // if so the customerID will be ignored for the remaining of the Import Job
                // an exception is added for product to be aware

                customersNoAccess.Add(customerID);
                var message =
                        $"FOR PRODUCT TO INVESTIGATE: The API returned a known error for Customer ID={customerID} - this customer will be removed from further calls for this Import job only. Add to Lookup {Constants.GOOGLEADS_KNOWN_INVALID_CUSTOMERS} to ignore this customer permanently.";
                LogWarning(message);
                hasError = true;
            });

        var hasData = streamLines > 0;

        if (!hasError && !hasData)
        {
            customersWithNoResult.Add(customerID);
        }
    }

    /// <summary>
    /// Build a sql query provided to the API
    /// </summary>
    public string BuildSql(APIReport<ReportSettings> report, Queue queue)
    {
        string sql = string.Empty;
        var sqlWhereClauses = new List<string>();
        string columns = string.Empty;
        string metrics = string.Empty;

        if (report.ReportFields?.Any() == true)
        {
            columns = $"{string.Join(",", report.ReportFields.Select(x => x.APIReportFieldName))}";
        }

        /*remove leading and trailing commas in case some columns are empty*/
        var fields = string.Join(",", columns, metrics).TrimStart(',').TrimEnd(',');

        var sqlSelect = $"select {fields} from {report.ReportSettings.ReportName.ToLower()}";

        if (report.ReportSettings.HasQueryFilter && !string.IsNullOrEmpty(this.filters))
            sqlWhereClauses.Add(this.filters);

        if (report.ReportSettings.HasQuerySegment)
            sqlWhereClauses.Add($"segments.date='{queue.FileDate.ToString("yyyy-MM-dd")}'");

        if (sqlWhereClauses.Count != 0)
        {
            var sqlWhere = sqlWhereClauses.Aggregate((partialClause, additionalClause) => partialClause + " and " + additionalClause);
            sql = $"{sqlSelect} where {sqlWhere}";
        }
        else
        {
            sql = sqlSelect;
        }

        if (report.ReportSettings.IncludeDrafts)
            sql = $"{sql} PARAMETERS include_drafts=true";

        return sql;
    }

    private List<CustomerClients> GetCustomersForAccount(IFileItem queueItem)
    {
        var endpoint = CurrentIntegration.EndpointURI.Replace("{ENTITYID}", queueItem.EntityID).TrimEnd('/');

        var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
        {
            Counter = 3,
            MaxRetry = this.maxRetry
        };

        var cancellableRetry = new CancellableRetry(queueItem.FileGUID.ToString(), apiCallsBackOffStrategy, this.apiExceptionsToCancel, runtime, this.maxRuntime);

        List<CustomerClientResponse> response = null;

        response = cancellableRetry.Execute(() =>
            this.googleAdsClient.DownloadAsJson<List<CustomerClientResponse>>(endpoint, this.getCustomersQuery, queueItem.FileGUID.ToString()));

        if (response == null)
        {
            throw new APIResponseException("Call to retrieve customers unsuccessful");
        }

        return response.First().Results;
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

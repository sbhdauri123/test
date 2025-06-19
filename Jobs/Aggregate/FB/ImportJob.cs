using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.DataSource.FB.Core;
using Greenhouse.Data.DataSource.FB.Data;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
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

namespace Greenhouse.Jobs.Aggregate.FB;

[Obsolete("This job is no longer being used in production", false)]
[Export("FB-AggregateImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private List<IFileItem> queueItems;
    private IEnumerable<APIReport<ReportSettings>> reports;
    private RemoteAccessClient rac;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private string JobGUID => base.JED.JobGUID.ToString();

    private string DeveloperToken => CurrentCredential.CredentialSet?.DeveloperToken?.ToString();
    private int _maxRetry;
    private Action<string> LogInfo;
    private Action<string> LogDebug;
    private Action<string> LogError;
    private Action<string, Exception> LogErrorExc;
    private List<string> errors;
    private int nbTopResult;
    private List<WebError> apiWebErrorList;
    private List<int> httpStatusNoRetry;
    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        Stage = Constants.ProcessingStage.RAW;
        base.Initialize();

        rac = GetS3RemoteAccessClient();
        baseRawDestUri = GetDestinationFolder();
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));

        LogInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        LogDebug = (msg) => logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid(msg)));
        LogError = (msg) => LogAndAddError(msg);
        LogErrorExc = (msg, exc) => LogAndAddError(msg, exc);

        LogInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        this.reports = JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        InitFromLookUp();

        this.errors = new List<string>();

        this.queueItems = JobService.GetActiveOrderedTopQueueItemsBySource(CurrentSource.SourceID, this.nbTopResult, this.JobLogger.JobLog.JobLogID)?.ToList();

        var apiCallsBackOffStrategy = new ExponentialBackOffStrategy()
        {
            Counter = 3,
            MaxRetry = this._maxRetry
        };

        Func<string, Policy> policy = (fileGuid) => GetPollyPolicy<Exception>(fileGuid, apiCallsBackOffStrategy);
    }
    private void InitFromLookUp()
    {
        this.nbTopResult = LookupService.GetQueueNBTopResultsForSource(CurrentSource.SourceID);

        if (int.TryParse(
            SetupService.GetById<Data.Model.Setup.Lookup>(Constants.FB_POLLY_MAX_RETRY)?.Value,
            out int maxRetry))
        {
            this._maxRetry = maxRetry;
        }
        else
        {
            this._maxRetry = 10;
        }

        //lookup containing an array of http status codes of errors and if call should be retried
        var webErrorLookup = SetupService.GetById<Data.Model.Setup.Lookup>(Constants.FB_WEB_ERROR_CODES);
        this.apiWebErrorList = string.IsNullOrEmpty(webErrorLookup?.Value) ? new List<WebError>() : ETLProvider.DeserializeType<List<WebError>>(webErrorLookup.Value);
        this.httpStatusNoRetry = this.apiWebErrorList.Where(x => !x.Retry).Select(x => (int)x.HttpStatusCode).ToList();
    }

    public void Execute()
    {
        LogInfo($"EXECUTE START {base.DefaultJobCacheKey}");

        if (this.queueItems.Count != 0)
        {
            var ownedAccountsDownloaded = new List<string>();
            foreach (Queue queueItem in this.queueItems)
            {
                try
                {
                    //make sure FileCollection is always empty at start of Import job, to avoid duplicate files from being added
                    queueItem.FileCollectionJSON = null;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                    var fileCollectionItems = new List<FileCollectionItem>(this.reports.Count());

                    if (!ownedAccountsDownloaded.Any(businessId => queueItem.EntityID == businessId))
                    {
                        var ownedAccountsList = GetOwnedAccounts(queueItem, fileCollectionItems, ownedAccountsDownloaded);
                    }

                    queueItem.Status = Constants.JobStatus.Complete.ToString();
                    queueItem.StatusId = (int)Constants.JobStatus.Complete;
                    queueItem.FileCollectionJSON = JsonConvert.SerializeObject(fileCollectionItems);
                    queueItem.FileSize = fileCollectionItems.Sum(f => f.FileSize);
                    JobService.Update(queueItem);
                }
                catch (HttpClientProviderRequestException exc)
                {
                    HandleException(queueItem, exc);
                }
                catch (Exception exc)
                {
                    HandleException(queueItem, exc);
                }
            }//end foreach queue 

            if (errors.Count > 0)
            {
                throw new ErrorsFoundException($"Total errors: {this.errors.Count}; Please check Splunk for more detail.");
            }
        }//end if queue.Any()
        else
        {
            LogInfo("There are no reports in the Queue");
        }

        LogInfo("Import job complete");
    }

    private void HandleException<TException>(Queue queueItem, TException exc) where TException : Exception
    {
        var logMsg = BuildLogMessage(queueItem, exc);
        LogErrorExc(logMsg, exc);
        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
    }
    private string BuildLogMessage<TException>(Queue queueItem, TException exc) where TException : Exception
    {
        return exc switch
        {
            HttpClientProviderRequestException httpEx =>
                $"Error - Import failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> Exception details : {httpEx}",
            _ =>
                $"Error - Import failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate} -> Exception: {exc.Message} - STACK {exc.StackTrace}"
        };
    }
    private List<AdAccountRow> GetOwnedAccounts(Queue queue, List<FileCollectionItem> fileCollectionItems, List<string> ownedAccountsDownloaded)
    {
        var ownedAccountReport = reports.FirstOrDefault(r => r.IsActive && r.ReportSettings.ReportType.Equals("businessownedaccount", StringComparison.InvariantCultureIgnoreCase));
        var allAdAccounts = new List<AdAccountRow>();

        if (ownedAccountReport == null)
            return allAdAccounts;

        var businessId = queue.EntityID.ToLower();

        string[] stagePaths =
        {
            businessId, GetDatedPartition(queue.FileDate), $"{queue.FileGUID}_{ownedAccountReport.ReportSettings.ReportType}.csv"
        };

        //get saved columns for all advertisers
        //store each response (JSON) in RAW
        //stage all saved column data in STAGE as one csv file
        var localStageFile = CreateLocalFile(stagePaths);

        var apiCallsBackOffStrategy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = _maxRetry
        };

        int fileCounter = 0;
        bool nextPage = false;
        string nextPageUrl = string.Empty;
        do
        {
            string reportResponse = CancellableWebCall(queue,
                () => RetrieveOwnedAccountsReportAsync(ownedAccountReport, businessId, nextPageUrl).GetAwaiter()
                    .GetResult(), apiCallsBackOffStrategy, httpStatusNoRetry, "RetrieveOwnedAccountsReport");

            string[] rawPaths =
            {
                businessId, GetDatedPartition(queue.FileDate), $"{queue.FileGUID}_{ownedAccountReport.ReportSettings.ReportType}_{fileCounter}.json"
            };

            var localRawFile = CreateLocalFile(rawPaths);
            using (StreamWriter output = new StreamWriter(localRawFile.FullName, false, Encoding.UTF8))
            {
                output.Write(reportResponse);
            }
            S3File rawFile = new S3File(RemoteUri.CombineUri(baseRawDestUri, rawPaths.ToArray()), GreenhouseS3Creds);
            base.UploadToS3(localRawFile, rawFile, rawPaths.ToArray());
            localRawFile.Delete();

            var accountReportFields = JsonConvert.DeserializeObject<ReportFields<AdAccount>>(reportResponse);
            nextPage = accountReportFields?.CursorPaging?.Next != null;
            if (nextPage)
            {
                nextPageUrl = accountReportFields.CursorPaging.Next;
            }

            //if any ad accounts then we add to list for staging
            if (accountReportFields.Data != null)
            {
                var adAccounts = accountReportFields.Data.ConvertAll(x => new AdAccountRow { BusinessManagerEntityID = businessId, AccountID = x.AccountId });
                allAdAccounts.AddRange(adAccounts);
            }

            fileCounter++;
            Task.Delay(300).Wait();
        } while (nextPage);

        if (allAdAccounts.Count != 0)
        {
            UtilsIO.WriteToCSV<AdAccountRow>(allAdAccounts, localStageFile.FullName);
            var stageFileCollection = new FileCollectionItem()
            {
                FilePath = localStageFile.Name,
                SourceFileName = ownedAccountReport.APIReportName,
                FileSize = localStageFile.Length
            };
            fileCollectionItems.Add(stageFileCollection);

            S3File stageFile = new S3File(RemoteUri.CombineUri(baseStageDestUri, stagePaths), GreenhouseS3Creds);
            base.UploadToS3(localStageFile, stageFile, stagePaths);
            queue.DeliveryFileDate = UtilsDate.GetLatestDateTime(queue.DeliveryFileDate, stageFile.LastWriteTimeUtc);
            localStageFile.Delete();
        }

        ownedAccountsDownloaded.Add(businessId);

        return allAdAccounts;
    }

    private async Task<string> RetrieveOwnedAccountsReportAsync(APIReport<ReportSettings> ownedAccountReport, string businessId, string nextPageUrl)
    {
        var url = nextPageUrl;
        if (string.IsNullOrEmpty(url))
        {
            var businessIdPlaceholder = "@businessid";
            var endpoint = ownedAccountReport.ReportSettings.URL.Replace(businessIdPlaceholder, businessId);
            url = $"{CurrentIntegration.EndpointURI}/{CurrentCredential.CredentialSet.Version}/{endpoint}";
        }

        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = url,
            Method = HttpMethod.Get,
            AuthToken = CurrentCredential.CredentialSet.AccessToken,
            ContentType = MediaTypeNames.Application.Json,
        });
    }

    private void LogAndAddError(string errorMessage, Exception exception = null)
    {
        if (exception == null)
        {
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(errorMessage)));
        }
        else
        {
            logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(errorMessage), exception));
        }
        this.errors.Add(errorMessage);
    }

    private FileSystemFile CreateLocalFile(string[] paths)
    {
        Uri localFileUri = RemoteUri.CombineUri(base.GetLocalImportDestinationFolder(), paths);

        var localFile = new FileSystemFile(localFileUri);
        if (!localFile.Directory.Exists)
        {
            localFile.Directory.Create();
        }

        return localFile;
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

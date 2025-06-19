using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.DataSource.FB.Core;
using Greenhouse.Data.DataSource.FB.Data;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using DateTime = System.DateTime;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Internal;

[Obsolete("This job is no longer being used in production", false)]
[Export("FB-InternalGetAdvertisers", typeof(IDragoJob))]
public class FBInternal : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private List<APIReport<ReportSettings>> reports;
    private APIReport<ReportSettings> businessOwnedReport;
    private RemoteAccessClient rac;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private string JobGUID => base.JED.JobGUID.ToString();
    private int maxRetry;
    private Action<string> LogInfo;
    private Action<string> LogDebug;
    private Action<string> LogError;
    private Action<string, Exception> LogErrorExc;
    private List<string> errors;
    private List<WebError> apiWebErrorList;
    private List<int> httpStatusNoRetry;
    private List<APIEntity> businessAccounts;
    private string guid;
    private DateTime date;
    private BaseRepository<APIEntity> apiEntityRepository;
    private int facebookSourceID;
    private string facebookSourceName;
    private IEnumerable<APIEntity> apiEntities;

    private const string OWNED_ACCOUNTS = "OwnedAccounts";
    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= base.HttpClientProvider;
        base.Initialize();

        rac = GetS3RemoteAccessClient();
        baseRawDestUri = GetDestinationFolder();
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));

        LogInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        LogDebug = (msg) => logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid(msg)));
        LogError = (msg) => LogAndAddError(msg);
        LogErrorExc = (msg, exc) => LogAndAddError(msg, exc);

        LogInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        businessAccounts = Data.Services.JobService.GetAllActiveAPIEntities(CurrentSource.SourceID).ToList();

        reports = Data.Services.JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId).ToList();

        businessOwnedReport = reports.Find(r => r.APIReportName == OWNED_ACCOUNTS);

        if (businessOwnedReport == null)
        {
            throw new APIReportException("No Active report 'OwnedAccounts' found");
        }

        InitFromLookUp();

        CurrentIntegration =
            Data.Services.SetupService.GetItems<Integration>(new { SourceId = CurrentSource.SourceID }).FirstOrDefault();

        this.errors = new List<string>();

        apiEntityRepository = new BaseRepository<APIEntity>();
        apiEntities = Data.Services.SetupService.GetItems<APIEntity>(new { SourceId = facebookSourceID });

        guid = Guid.NewGuid().ToString();
        date = DateTime.Now;

        LogInfo($"For this job instance, GUID={guid} Date={date.ToString("d")}");
    }

    private void InitFromLookUp()
    {
        if (int.TryParse(
            Data.Services.SetupService.GetById<Lookup>(Constants.FB_POLLY_MAX_RETRY)?.Value,
            out int maxRetry))
        {
            this.maxRetry = maxRetry;
        }
        else
        {
            this.maxRetry = 10;
        }

        //lookup containing an array of http status codes of errors and if call should be retried
        var webErrorLookup = Data.Services.SetupService.GetById<Lookup>(Constants.FB_WEB_ERROR_CODES);
        this.apiWebErrorList = string.IsNullOrEmpty(webErrorLookup?.Value) ? new List<WebError>() : ETLProvider.DeserializeType<List<WebError>>(webErrorLookup.Value);
        this.httpStatusNoRetry = this.apiWebErrorList.Where(x => !x.Retry).Select(x => (int)x.HttpStatusCode).ToList();

        facebookSourceName = Data.Services.SetupService.GetById<Lookup>(Common.Constants.FACEBOOK_SOURCE_NAME).Value;
        facebookSourceID = Data.Services.SetupService.GetItems<Source>(new { SourceName = facebookSourceName }).First().SourceID;
    }

    public void Execute()
    {
        LogInfo($"EXECUTE START {base.DefaultJobCacheKey}");

        var entitiesToCreate = new List<APIEntity>();
        var stagedFileNames = new List<IFile>();
        foreach (var business in businessAccounts)
        {
            try
            {
                var accounts = GetOwnedAccounts(business.APIEntityCode);

                if (accounts.Count == 0)
                {
                    LogInfo($"No account found for Business Account {business.APIEntityCode}");
                    continue;
                }

                GetAccountNames(business.APIEntityCode, accounts);

                IFile stageFile = StageFiles(business.APIEntityCode, accounts);
                if (stageFile != null)
                {
                    stagedFileNames.Add(stageFile);
                }

                var missingAccounts = accounts.Where(a => !apiEntities.Select(api => api.APIEntityCode).Contains(a.AccountID)).ToList();

                if (missingAccounts.Count == 0)
                {
                    LogInfo($"No NEW account found for Business Account {business.APIEntityCode}");
                    continue;
                }

                var toCreate = CreateAPIEntities(missingAccounts);
                entitiesToCreate.AddRange(toCreate);
            }
            catch (HttpClientProviderRequestException exc)
            {
                LogErrorExc($"Error - Import failed for EntityID: {business.APIEntityCode} -> Exception details : {exc}", exc);
            }
            catch (Exception exc)
            {
                LogErrorExc(
                    $"Error - Import failed for EntityID: {business.APIEntityCode} -> Exception: {exc.Message} - STACK {exc.StackTrace}",
                    exc);
            }
        }

        try
        {
            // final steps: creating the newly found APIEntities and Process the files in S3 to Redshift
            if (stagedFileNames.Count != 0)
            {
                // we start by processing the files in staging
                // if that step fails, the entities wont be inserted in APIEntity table
                // that table is used to determinate if an advertiser account has been already added 
                LogInfo($"{stagedFileNames.Count} files to process");
                ProcessFiles(stagedFileNames);
            }
            else
            {
                LogInfo($"No file to process");
            }

            if (entitiesToCreate.Count != 0)
            {
                LogInfo($"{entitiesToCreate.Count} Entities to create");
                apiEntityRepository.BulkInsert(entitiesToCreate, "ApiEntity");
            }
            else
            {
                LogInfo($"No entity to create");
            }
        }
        catch (Exception exc)
        {
            LogAndAddError(exc.Message, exc);
        }

        if (errors.Count > 0)
        {
            throw new ErrorsFoundException($"Total errors: {this.errors.Count}; Please check Splunk for more detail.");
        }

        LogInfo("Job FB-GetAccountsJob complete");
    }

    private void ProcessFiles(List<IFile> stagedFileNames)
    {
        string etlScriptName = $"redshiftload{CurrentSource.SourceName.ToLower()}.sql";

        var etlFullPath = new string[] {
            "scripts", "etl", "redshift"
            , CurrentSource.SourceName.ToLower()
            , etlScriptName };

        var sql = ETLProvider.GetRedshiftScripts(RootBucket, etlFullPath);

        var path = baseStageDestUri + "/" + GetDatedPartition(date);
        var name = stagedFileNames.First().Name;

        var odbcParams = base.GetScriptParameters(path, guid, null).ToList();
        odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = $"sourcefile-OwnedAccounts", Value = name });
        odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
        odbcParams.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });

        //PROCESS load
        var finalSql = RedshiftRepository.PrepareCommandText(sql, odbcParams);
        RedshiftRepository.ExecuteRedshiftCommand(finalSql);
    }

    private List<APIEntity> CreateAPIEntities(List<AdAccountRow> accounts)
    {
        var entitiesToCreate = new List<APIEntity>();

        foreach (var account in accounts)
        {
            var apiEntity = apiEntities.FirstOrDefault(a => a.APIEntityCode == account.AccountID);

            if (apiEntity == null)
            {
                apiEntity = new APIEntity
                {
                    APIEntityCode = account.AccountID,
                    APIEntityName = account.AccountName,
                    SourceID = facebookSourceID,
                    BackfillPriority = false,
                    CreatedDate = DateTime.Now,
                    LastUpdated = DateTime.Now,
                    IsActive = true,
                    StartDate = DateTime.Now
                };

                entitiesToCreate.Add(apiEntity);
            }
        }

        return entitiesToCreate;
    }

    private void GetAccountNames(string businessID, List<AdAccountRow> accounts)
    {
        foreach (var account in accounts)
        {
            var response = GetAdvertiserNameAsync(account.AccountID).GetAwaiter().GetResult();
            string[] rawPaths =
            {
                GetDatedPartition(date), $"{guid}_{businessID}_{account.AccountID}_GetAdvertiserName.json"
            };

            var localRawFile = CreateLocalFile(rawPaths);
            using (StreamWriter output = new StreamWriter(localRawFile.FullName, false, Encoding.UTF8))
            {
                output.Write(response);
            }
            S3File rawFile = new S3File(RemoteUri.CombineUri(baseRawDestUri, rawPaths.ToArray()), GreenhouseS3Creds);
            base.UploadToS3(localRawFile, rawFile, rawPaths.ToArray());
            localRawFile.Delete();

            var accountInfo = JsonConvert.DeserializeObject<GetAccountInfo>(response);

            account.AccountName = accountInfo.Name;
            Task.Delay(300).Wait();
        }
    }

    //Consolidate all reports into the BusinessOwnedReport CSV
    private S3File StageFiles(string businessID, List<AdAccountRow> accounts)
    {
        if (accounts.Count != 0)
        {
            string[] stagePaths =
            {
                GetDatedPartition(date), $"{guid}_{businessID}_{businessOwnedReport.ReportSettings.ReportType}.csv"
            };

            var localStageFile = CreateLocalFile(stagePaths);
            UtilsIO.WriteToCSV<AdAccountRow>(accounts, localStageFile.FullName);

            S3File stageFile = new S3File(RemoteUri.CombineUri(baseStageDestUri, stagePaths), GreenhouseS3Creds);
            base.UploadToS3(localStageFile, stageFile, stagePaths);
            localStageFile.Delete();
            return stageFile;
        }

        return null;
    }

    private List<AdAccountRow> GetOwnedAccounts(string businessID)
    {
        var allAdAccounts = new List<AdAccountRow>();

        //get saved columns for all advertisers
        //store each response (JSON) in RAW
        //stage all saved column data in STAGE as one csv file

        var apiCallsBackOffStrategy = new BackOffStrategy
        {
            Counter = 0,
            MaxRetry = 10
        };

        foreach (var report in reports)
        {
            int fileCounter = 0;
            bool nextPage = false;
            string nextPageUrl = string.Empty;

            do
            {
                string reportResponse = CancellableWebCall(
                    () => RetrieveOwnedAccountsReportAsync(report, businessID, nextPageUrl).GetAwaiter().GetResult(),
                    apiCallsBackOffStrategy, httpStatusNoRetry, "RetrieveOwnedAccountsReport",
                    $"Internal FBGetAccountsJob guid={guid}");

                string[] rawPaths =
                {
                    GetDatedPartition(date), $"{guid}_{businessID}_{report.ReportSettings.ReportType}_{fileCounter}.json"
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

                //Ensure that we only add unique accounts for staging
                if (accountReportFields.Data != null)
                {
                    var adAccounts = accountReportFields.Data.ConvertAll(x => new AdAccountRow { BusinessManagerEntityID = businessID, AccountID = x.AccountId });
                    adAccounts.ForEach(adAccount =>
                    {
                        if (!allAdAccounts.Any(x => x.AccountID == adAccount.AccountID))
                        {
                            allAdAccounts.Add(adAccount);
                        }
                    });
                }

                fileCounter++;
                Task.Delay(300).Wait();
            } while (nextPage);
        }

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

        //var reportRequest = ExternalDataProvider.GetWebRequest(accessToken: CurrentCredential.CredentialSet.AccessToken, uri: new Uri(url), method: "GET", accept: null, contentType: "application/json");
        //var reportResponse = ExternalDataProvider.GetHttpWebResponse(reportRequest, LogError);
        //return reportResponse;
        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = url,
            Method = HttpMethod.Get,
            AuthToken = CurrentCredential.CredentialSet.AccessToken,
            ContentType = MediaTypeNames.Application.Json,
        });
    }

    private async Task<string> GetAdvertiserNameAsync(string accountId)
    {
        //var reportRequest = ExternalDataProvider.GetWebRequest(accessToken: CurrentCredential.CredentialSet.AccessToken, uri: new Uri(url), method: "GET", accept: null, contentType: "application/json");
        //var reportResponse = ExternalDataProvider.GetHttpWebResponse(reportRequest, LogError);
        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri =
                $"{CurrentIntegration.EndpointURI}/{CurrentCredential.CredentialSet.Version}/act_{accountId}?fields=id,name",
            Method = HttpMethod.Get,
            AuthToken = CurrentCredential.CredentialSet.AccessToken,
            ContentType = MediaTypeNames.Application.Json,
        });
    }

    private void LogAndAddError(string errorMessage, Exception exception = null)
    {
        logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid(errorMessage), exception));
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

    ~FBInternal()
    {
        Dispose(false);
    }
}

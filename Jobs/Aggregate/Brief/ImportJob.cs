using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.DataSource.Brief;
using Greenhouse.Data.DataSource.Brief;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DateTime = System.DateTime;
using HttpRequestOptions = Greenhouse.Utilities.HttpRequestOptions;

namespace Greenhouse.Jobs.Aggregate.Brief;

[Export("BriefImportJob", typeof(IDragoJob))]
public class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
{
    private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private IEnumerable<Greenhouse.Data.Model.Aggregate.APIReport<ReportSettings>> apiReports;
    private RemoteAccessClient rac;
    private Uri baseRawDestUri;
    private Uri baseStageDestUri;
    private int _maxRetry;
    private Action<string> LogInfo;
    private Action<string> LogError;
    private Action<string, Exception> LogErrorExc;
    private List<string> errors;
    private List<DataManagerWebError> apiWebErrorList;
    private List<int> httpStatusNoRetry;
    private Guid guid;
    private DateTime runDate;
    private List<APIEntity> apiEntities;
    private DataManagerOAuth dataManagerOAuth;
    private BriefOAuth briefOAuth;
    private List<IFileItem> importedFiles;
    private IHttpClientProvider _httpClientProvider;

    public void PreExecute()
    {
        _httpClientProvider ??= HttpClientProvider;
        base.Initialize();

        rac = GetS3RemoteAccessClient();
        baseRawDestUri = GetDestinationFolder();
        baseStageDestUri = new Uri(baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));

        LogInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));
        LogError = (msg) => LogAndAddError(msg);
        LogErrorExc = (msg, exc) => LogAndAddError(msg, exc);

        LogInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");

        this.apiReports = Data.Services.JobService.GetAllActiveAPIReports<ReportSettings>(base.SourceId);

        if (!apiReports.Any())
        {
            throw new APIReportException("No Active Brief reports found");
        }

        apiEntities = Data.Services.JobService.GetAllActiveAPIEntities(CurrentSource.SourceID).ToList();

        InitFromLookUp();

        CurrentIntegration =
            Data.Services.SetupService.GetItems<Integration>(new { SourceId = CurrentSource.SourceID }).FirstOrDefault();

        this.errors = new List<string>();

        briefOAuth = new BriefOAuth(_httpClientProvider, CurrentCredential);

        // Get data manager credential
        // keeping this here for now in case Data manager release on 2023-07-07 needs to be rolled back
        // if it is successful, then the separate Data Manager OAuth can be removed completely
        // and just use the OS-Auth (ie current credential) to authorize data manager api calls
        var dataManagerCredentialId = apiReports.FirstOrDefault(x => x.ReportSettings.ReportType == "ClientMetadata");
        var dataManagerCredential = Data.Services.SetupService.GetById<Credential>(dataManagerCredentialId?.CredentialID) ?? CurrentCredential;
        dataManagerOAuth = new DataManagerOAuth(_httpClientProvider, dataManagerCredential, CurrentIntegration.EndpointURI);

        importedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID).ToList();
    }

    private void InitFromLookUp()
    {
        if (int.TryParse(
            Data.Services.SetupService.GetById<Lookup>(Constants.BRIEF_POLLY_MAX_RETRY)?.Value,
            out int maxRetry))
        {
            this._maxRetry = maxRetry;
        }
        else
        {
            this._maxRetry = 10;
        }

        //lookup containing an array of http status codes of errors and if call should be retried
        var webErrorLookup = Data.Services.SetupService.GetById<Lookup>(Constants.DATAMANAGER_WEB_ERROR_CODES);
        this.apiWebErrorList = string.IsNullOrEmpty(webErrorLookup?.Value) ? new List<DataManagerWebError>() : ETLProvider.DeserializeType<List<DataManagerWebError>>(webErrorLookup.Value);
        this.httpStatusNoRetry = this.apiWebErrorList.Where(x => !x.Retry).Select(x => (int)x.HttpStatusCode).ToList();
    }

    public void Execute()
    {
        LogInfo($"EXECUTE START {base.DefaultJobCacheKey}");

        if (apiEntities.Count != 0)
        {
            foreach (var apiEntity in apiEntities)
            {
                try
                {
                    guid = Guid.NewGuid();
                    runDate = DateTime.Now;
                    LogInfo($"For this job instance, GUID={guid} Date={runDate.ToString("d")} Entity: {apiEntity.APIEntityName}-({apiEntity.APIEntityCode})");

                    var fileCollection = new List<FileCollectionItem>();
                    DateTime? deliveryFileDate = runDate;

                    //check if job should run
                    var skipJobRun = false;
                    var entityImportedFiles = importedFiles.Where(x => x.EntityID == apiEntity.APIEntityCode);
                    var fileDate = runDate.ToUniversalTime().Date;
                    if (entityImportedFiles != null && entityImportedFiles.Any())
                    {
                        // skip if data was imported for the current date
                        var latestImportedFileDate = entityImportedFiles.Max(x => x.FileDate);
                        skipJobRun = latestImportedFileDate.Subtract(fileDate).Days == 0;
                    }
                    else
                    {
                        // skip if start date is in future
                        var fileStartDate = apiEntity.StartDate?.Date ?? CurrentIntegration.FileStartDate.Date;
                        skipJobRun = fileDate < fileStartDate;
                    }

                    if (skipJobRun)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Skipping Entity: {apiEntity.APIEntityName}-({apiEntity.APIEntityCode}) for file date: {fileDate}")));
                        continue;
                    }

                    var reports = GetBriefData(apiEntity, out var reportDate);
                    if (reports.Count != 0)
                    {
                        fileCollection.AddRange(reports);
                        deliveryFileDate = UtilsDate.GetLatestDateTime(deliveryFileDate, reportDate);
                    }

                    if (fileCollection.Count != 0)
                    {
                        var importFile = new Queue()
                        {
                            FileGUID = guid,
                            FileDate = runDate,
                            SourceFileName = $"{CurrentSource.SourceName}Reports",
                            FileName = $"{CurrentSource.SourceName}Reports_{runDate.ToString("yyyyMMdd")}_{apiEntity.APIEntityCode}",
                            FileCollectionJSON = Newtonsoft.Json.JsonConvert.SerializeObject(fileCollection),
                            FileSize = fileCollection.Sum(x => x.FileSize),
                            EntityID = apiEntity.APIEntityCode.ToLower(),
                            IntegrationID = CurrentIntegration.IntegrationID,
                            SourceID = CurrentSource.SourceID,
                            Status = Constants.JobStatus.Complete.ToString(),
                            StatusId = (int)Constants.JobStatus.Complete,
                            JobLogID = this.JobLogger.JobLog.JobLogID,
                            Step = JED.Step.ToString(),
                            DeliveryFileDate = deliveryFileDate
                        };

                        LogInfo($"{guid} - Adding to queue - FileDate: {runDate}");
                        Data.Services.JobService.Add<IFileItem>(importFile);
                    }
                }
                catch (HttpClientProviderRequestException exception)
                {
                    LogErrorExc($"Error - Import failed for apiEntity code: {apiEntity.APIEntityCode} -> Exception details : {exception}", exception);
                }
                catch (Exception exc)
                {
                    LogErrorExc($"Error - Import failed for apiEntity code: {apiEntity.APIEntityCode} -> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                }
            }
        }
        else
        {
            LogInfo("There are no api entities available");
        }

        if (errors.Count > 0)
        {
            throw new ErrorsFoundException($"Total errors: {this.errors.Count}; Please check Splunk for more detail.");
        }

        LogInfo("Brief Import job complete");
    }

    private List<FileCollectionItem> GetBriefData(APIEntity apiEntity, out DateTime? deliveryFileDate)
    {
        var fileCollection = new List<FileCollectionItem>();
        deliveryFileDate = DateTime.Now;

        // aarc info formatted as "1-2-3-4" in api entity code
        var aarcIds = apiEntity.APIEntityCode.Split('-');
        if (aarcIds.Length != 4)
        {
            LogError($"API AARC Entity is not properly formatted {apiEntity.APIEntityCode} - expecting four IDs hyphenated; EX: 1-2-3-4 " +
                $"AKA mappedAgencyDivisionID-mappedBusinessUnitID-mappedRegionID-mappedCountryID");
            return fileCollection;
        }
        var aarcEntity = new AarcEntity
        {
            MappedAgencyDivisionID = aarcIds[0],
            MappedBusinessUnitID = aarcIds[1],
            MappedRegionID = aarcIds[2],
            MappedCountryID = aarcIds[3]
        };

        foreach (var report in apiReports)
        {
            var apiCallsBackOffStrategy = new BackOffStrategy
            {
                Counter = 0,
                MaxRetry = _maxRetry
            };
            var reportResponse = string.Empty;

            FileCollectionItem fileItem;
            DateTime reportWriteTime;
            var fileItems = new List<FileCollectionItem>();

            LogInfo($"Getting {report.ReportSettings.ReportType}-{report.ReportSettings.EntityName} for mappedAgencyDivisionID: {aarcEntity.MappedAgencyDivisionID} - mappedBusinessUnitID: {aarcEntity.MappedBusinessUnitID}" +
                $"mappedRegionID: {aarcEntity.MappedRegionID} - mappedCountryID: {aarcEntity.MappedCountryID}");

            var emptySpace = @"[\s]+";
            var entityName = Regex.Replace(report.ReportSettings.EntityName, emptySpace, "");

            if (report.ReportSettings.ReportType == "ClientMetadata")
            {
                fileItems = GetAndDownloadClientMetadata(report, aarcEntity, apiEntity, apiCallsBackOffStrategy, entityName, out reportWriteTime);
            }
            else if (report.ReportSettings.ReportType == "Brief" && report.ReportSettings.EntityName == "artifacts")
            {
                fileItems = GetAndDownloadBriefData(report, aarcEntity, apiEntity, apiCallsBackOffStrategy, entityName, out reportWriteTime);
            }
            else if (report.ReportSettings.ReportType == "Brief" && report.ReportSettings.EntityName == "status")
            {
                fileItems = GetAndDownloadBriefStatus(report, aarcEntity, apiEntity, apiCallsBackOffStrategy, entityName, out reportWriteTime);
            }
            else
            {
                throw new APIReportException($"API Report Type {report.ReportSettings.ReportType}-{report.ReportSettings.EntityName} is not configured");
            }

            if (report.ReportSettings.StageJsonArray)
            {
                // Redshift parses JSON in a particular way
                // in the case of brief status, we just have an array of objects
                // we name the array in the staged file in order to use the JSON path setting in the COPY command
                var stageFileItems = StageReport(fileItems, apiEntity, report);
                fileItem = CreateManifestFile(apiEntity, stageFileItems, $"{report.ReportSettings.ReportType}_{entityName}", true);
            }
            else
            {
                fileItem = CreateManifestFile(apiEntity, fileItems, $"{report.ReportSettings.ReportType}_{entityName}");
            }

            fileCollection.Add(fileItem);
            deliveryFileDate = reportWriteTime;

            Task.Delay(300).Wait();
        }

        return fileCollection;
    }

    private FileCollectionItem SaveFile(APIEntity apiEntity, string fileName, APIReport<ReportSettings> report, string reportResponse, out DateTime deliveryFiledate, bool stageFile = false)
    {
        var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
        var fileNameWithExtension = $"{fileNameWithoutExtension}.{report.ReportSettings.Extension}";

        string[] rawPaths =
        {
                apiEntity.APIEntityCode.ToLower(),
                GetDatedPartition(runDate),
                fileNameWithExtension
        };

        var localRawFile = CreateLocalFile(rawPaths);
        using (StreamWriter output = new StreamWriter(localRawFile.FullName, false, new UTF8Encoding(false)))
        {
            output.Write(reportResponse);
        }

        var destinationUri = stageFile ? baseStageDestUri : baseRawDestUri;
        var rawFile = new S3File(RemoteUri.CombineUri(destinationUri, rawPaths.ToArray()), GreenhouseS3Creds);
        base.UploadToS3(localRawFile, rawFile, rawPaths.ToArray());
        localRawFile.Delete();

        var fileItem = new FileCollectionItem()
        {
            FileSize = rawFile.Length,
            SourceFileName = $"{report.ReportSettings.ReportType}_{report.ReportSettings.EntityName}",
            FilePath = fileNameWithExtension
        };

        deliveryFiledate = rawFile.LastWriteTimeUtc;

        return fileItem;
    }

    private List<FileCollectionItem> DownloadReport<T>(APIEntity apiEntity, string fileName, APIReport<ReportSettings> report, string reportResponse, out DateTime deliveryFiledate, bool stageFile = false, List<T> objectList = null)
    {
        deliveryFiledate = DateTime.UtcNow;
        List<FileCollectionItem> fileItems = new List<FileCollectionItem>();

        if (objectList != null && objectList.Count != 0 && report.ReportSettings.objectListSize > 0 && !stageFile)
        {
            var subLists = UtilsText.GetSublistFromList(objectList, report.ReportSettings.objectListSize);
            var subListCounter = 0;
            foreach (var objects in subLists)
            {
                var jsonBatch = Newtonsoft.Json.JsonConvert.SerializeObject(objects);
                var filePart = SaveFile(apiEntity, $"{fileName}_{subListCounter}", report, jsonBatch, out var filePartDate, stageFile);
                fileItems.Add(filePart);

                deliveryFiledate = filePartDate;

                subListCounter++;
            }

            return fileItems;
        }

        var fileItem = SaveFile(apiEntity, fileName, report, reportResponse, out var fileDate, stageFile);
        fileItems.Add(fileItem);

        deliveryFiledate = fileDate;

        return fileItems;
    }

    private List<FileCollectionItem> GetAndDownloadClientMetadata(APIReport<ReportSettings> report, AarcEntity aarcEntity, APIEntity apiEntity, BackOffStrategy apiCallsBackOffStrategy
        , string entityName, out DateTime deliveryFileDate)
    {
        var fileName = $"{guid}_{report.ReportSettings.ReportType}_{entityName}_{aarcEntity.MappedAgencyDivisionID}_{aarcEntity.MappedBusinessUnitID}_{aarcEntity.MappedRegionID}_{aarcEntity.MappedCountryID}";

        var reportResponse = string.Empty;
        var endpoint = $"{CurrentIntegration.EndpointURI.TrimEnd('/')}/{report.ReportSettings.Path.TrimEnd('/')}" +
            $"?masterAgencyDivisionID={aarcEntity.MappedAgencyDivisionID}&masterBusinessUnitID={aarcEntity.MappedBusinessUnitID}" +
            $"&entityName={report.ReportSettings.EntityName}";
        reportResponse = CancellableWebCall<string>(() => GetClientMetadataAsync(report, endpoint).GetAwaiter().GetResult(), apiCallsBackOffStrategy, httpStatusNoRetry
                         , "RetrieveClientMetadata", $"BriefImportJob guid={guid}");
        // wrap json in an outer object to load complex json heirarcy into Redshift
        reportResponse = $"{{\"all\":{reportResponse}}}";

        var fileItems = DownloadReport<string>(apiEntity, fileName, report, reportResponse, out var lastWriteTime, false, null);
        deliveryFileDate = lastWriteTime;
        return fileItems;
    }

    private async Task<string> GetClientMetadataAsync(APIReport<ReportSettings> clientMetadataReport, string endpoint)
    {
        string accessToken = GetDataManagerToken();
        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = endpoint,
            Method = new HttpMethod(clientMetadataReport.ReportSettings.Method),
            AuthToken = accessToken,
            ContentType = "application/json"
        });
    }

    private List<FileCollectionItem> GetAndDownloadBriefData(APIReport<ReportSettings> report, AarcEntity aarcEntity, APIEntity apiEntity, BackOffStrategy apiCallsBackOffStrategy, string entityName, out DateTime deliveryFileDate)
    {
        var hasNextPage = true;
        var counter = 0;
        deliveryFileDate = DateTime.Now;
        var fileItems = new List<FileCollectionItem>();
        var fileName = $"{guid}_{report.ReportSettings.ReportType}_{entityName}_{aarcEntity.MappedAgencyDivisionID}_{aarcEntity.MappedBusinessUnitID}_{aarcEntity.MappedRegionID}_{aarcEntity.MappedCountryID}";

        while (hasNextPage)
        {
            var reportResponse = string.Empty;

            var endpoint = $"{briefOAuth.Host.TrimEnd('/')}/{report.ReportSettings.Path.TrimEnd('/')}";

            // add parameter and paging if configured in report settings
            string queryString = SetParameters(report, counter);

            endpoint = ReplaceTags(string.IsNullOrEmpty(queryString) ? endpoint : $"{endpoint}?{queryString.TrimStart(Constants.AMPERSAND_ARRAY)}", aarcEntity);

            reportResponse = CancellableWebCall<string>(() => GetBriefJsonDataAsync(report, endpoint).GetAwaiter().GetResult(), apiCallsBackOffStrategy, httpStatusNoRetry
                         , "GetBriefJsonData", $"BriefImportJob guid={guid}");

            var artifact = ETLProvider.DeserializeType<SearchArtifactResponse>(reportResponse);

            if (report.ReportSettings?.PageSize > 0)
            {
                if (artifact != null)
                {
                    hasNextPage = artifact.Total > ((counter + 1) * report.ReportSettings.PageSize);
                }
                else
                {
                    hasNextPage = false;
                }
            }
            else
            {
                hasNextPage = false;
            }

            var savedFiles = DownloadReport(apiEntity, $"{fileName}_pg{counter}", report, "[]", out var lastWriteTime, false, artifact.Artifacts);
            fileItems.AddRange(savedFiles);
            deliveryFileDate = lastWriteTime;
            counter++;
        }

        return fileItems;
    }

    private List<FileCollectionItem> GetAndDownloadBriefStatus(APIReport<ReportSettings> report, AarcEntity aarcEntity, APIEntity apiEntity, BackOffStrategy apiCallsBackOffStrategy, string entityName, out DateTime deliveryFileDate)
    {
        deliveryFileDate = DateTime.Now;
        var fileItems = new List<FileCollectionItem>();
        var fileName = $"{guid}_{report.ReportSettings.ReportType}_{entityName}_{aarcEntity.MappedAgencyDivisionID}_{aarcEntity.MappedBusinessUnitID}_{aarcEntity.MappedRegionID}_{aarcEntity.MappedCountryID}";

        var reportResponse = string.Empty;

        var endpoint = $"{briefOAuth.Host.TrimEnd('/')}/{report.ReportSettings.Path.TrimEnd('/')}";

        // add parameters
        string queryString = SetParameters(report);

        endpoint = ReplaceTags(string.IsNullOrEmpty(queryString) ? endpoint : $"{endpoint}?{queryString.TrimStart(Constants.AMPERSAND_ARRAY)}", aarcEntity);

        reportResponse = CancellableWebCall<string>(() => GetBriefJsonDataAsync(report, endpoint).GetAwaiter().GetResult(), apiCallsBackOffStrategy, httpStatusNoRetry
                        , "GetBriefJsonData", $"BriefImportJob guid={guid}");

        var savedFiles = DownloadReport<string>(apiEntity, fileName, report, reportResponse, out var lastWriteTime, false);
        fileItems.AddRange(savedFiles);
        deliveryFileDate = lastWriteTime;

        return fileItems;
    }

    private List<FileCollectionItem> StageReport(List<FileCollectionItem> fileItems, APIEntity entity, APIReport<ReportSettings> report)
    {
        var stageFileItems = new List<FileCollectionItem>();
        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"Staging {report.APIReportName} Report for account id: {entity.APIEntityCode}; fileGUID: {guid}")));

        foreach (var file in fileItems)
        {
            string[] paths = new string[]
            {
                entity.APIEntityCode.ToLower(), GetDatedPartition(runDate), file.FilePath
            };

            IFile rawFile = rac.WithFile(RemoteUri.CombineUri(baseRawDestUri, paths));
            string rawText;
            using (var sr = new System.IO.StreamReader(rawFile.Get()))
            {
                rawText = sr.ReadToEnd();
            }

            var reports = DownloadReport<string>(entity, file.FilePath, report, $"{{\"allData\":{rawText}}}", out var lastWriteTime, true);
            stageFileItems.AddRange(reports);
        }

        return stageFileItems;
    }

    private static string ReplaceTags(string endpoint, AarcEntity aarcEntity)
    {
        var newEndpoint = string.Empty;

        var mappedAgencyDivisionIDTag = "@mappedAgencyDivisionID";
        var mappedBusinessUnitIDTag = "@mappedBusinessUnitID";
        var mappedRegionIDTag = "@mappedRegionID";
        var mappedCountryIDTag = "@mappedCountryID";

        newEndpoint = endpoint.Replace(mappedAgencyDivisionIDTag, aarcEntity.MappedAgencyDivisionID)
            .Replace(mappedBusinessUnitIDTag, aarcEntity.MappedBusinessUnitID)
            .Replace(mappedRegionIDTag, aarcEntity.MappedRegionID)
            .Replace(mappedCountryIDTag, aarcEntity.MappedCountryID);

        return newEndpoint;
    }

    private static string SetParameters(APIReport<ReportSettings> report, int counter = 0)
    {
        var parameters = new List<string>();

        if (report.ReportSettings?.PageSize > 0)
        {
            var startingArtifact = counter * report.ReportSettings.PageSize;
            parameters.Add($"from={startingArtifact}&size={report.ReportSettings.PageSize}");
        }

        if (!string.IsNullOrEmpty(report.ReportSettings.Parameters))
        {
            parameters.Add($"{report.ReportSettings.Parameters}");
        }

        var queryString = string.Join("&", parameters);
        return queryString;
    }

    private async Task<string> GetBriefJsonDataAsync(APIReport<ReportSettings> report, string endpoint)
    {
        string accessToken = briefOAuth.BriefAccessToken;
        return await _httpClientProvider.SendRequestAsync(new HttpRequestOptions
        {
            Uri = endpoint,
            Method = new HttpMethod(report.ReportSettings.Method),
            AuthToken = accessToken,
            ContentType = "application/json",
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

    private FileCollectionItem CreateManifestFile(APIEntity apiEntity, List<FileCollectionItem> fileItems, string fileType, bool isStageFile = false)
    {
        var manifest = new Data.Model.Setup.RedshiftManifest();

        var destinationUri = isStageFile ? baseStageDestUri : baseRawDestUri;

        foreach (var file in fileItems)
        {
            var s3File = $"{destinationUri.OriginalString.TrimStart('/')}/{apiEntity.APIEntityCode.ToLower()}/{GetDatedPartition(runDate)}/{file.FilePath}";
            manifest.AddEntry(s3File, true);
        }

        var fileName = $"{guid}_{fileType}.manifest";
        var manifestPath = GetManifestFilePath(apiEntity, fileName);
        var manifestFilePath = ETLProvider.GenerateManifestFile(manifest, this.RootBucket, manifestPath);

        var fileItem = new FileCollectionItem()
        {
            FileSize = fileItems.Sum(file => file.FileSize),
            SourceFileName = fileType,
            FilePath = fileName
        };

        return fileItem;
    }

    private string[] GetManifestFilePath(APIEntity apiEntity, string name)
    {
        string[] manifestPath = new string[]
        {
            apiEntity.APIEntityCode.ToLower(),
            GetDatedPartition(runDate)
        };

        var manifestUri = RemoteUri.CombineUri(baseRawDestUri, manifestPath);
        return new string[]
        {
            manifestUri.AbsolutePath, name
        };
    }

    /// <summary>
    /// Leading up to Data Manager Release 2023.07.07
    /// Auth token is user-name based
    /// Post release the endpoints will use OS Auth
    /// </summary>
    /// <returns></returns>
    private string GetDataManagerToken()
    {
        if (!string.IsNullOrEmpty(dataManagerOAuth.Username))
        {
            return dataManagerOAuth.DataManagerAccessToken;
        }

        return briefOAuth.BriefAccessToken;
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

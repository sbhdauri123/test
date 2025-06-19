using Amazon.S3;
using Amazon.S3.Transfer;
using Greenhouse.Auth;
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Mail;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using Polly;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Framework
{
    public abstract class BaseFrameworkJob : BaseDragoJob
    {
        protected Constants.ProcessingStage Stage { get; set; }
        private IEnumerable<Credential> _credentials;
        private static readonly object locker = new object();
        private readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        protected string S3Protocol { get; set; } = Constants.URI_SCHEME_S3;

        protected Uri GetDestinationFolder()
        {
            Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);

            string[] paths = new string[] {
                this.Stage.ToString().ToLower(),
                CurrentSource.SourceName.Replace(" ", string.Empty).ToLower() };
            return RemoteUri.CombineUri(baseUri, paths);
        }

        protected Uri GetLocalTransformDestinationFolder()
        {
            string path = Path.Combine(
                Configuration.Settings.Current.Greenhouse.GreenhouseTransformPath,
                Stage.ToString().ToLower(),
                CurrentSource.SourceName.ToLower());

            return Uri.TryCreate(path, UriKind.Absolute, out Uri absoluteUri)
                ? absoluteUri
                : new Uri(Path.Combine(Directory.GetCurrentDirectory(), path));
        }

        protected Uri GetLocalImportDestinationFolder()
        {
            string path = Path.Combine(
                Configuration.Settings.Current.Greenhouse.GreenhouseImportPath,
                Stage.ToString().ToLower(),
                CurrentSource.SourceName.ToLower());

            return Uri.TryCreate(path, UriKind.Absolute, out Uri absoluteUri)
                ? absoluteUri
                : new Uri(Path.Combine(Directory.GetCurrentDirectory(), path));
        }

        protected int SourceId
        {
            get
            {
                return Convert.ToInt32(this.GetUserSelection(Constants.US_SOURCE_ID));
            }
        }

        protected int IntegrationId
        {
            get
            {
                return Convert.ToInt32(this.GetUserSelection(Constants.US_INTEGRATION_ID));
            }
        }

        private Source _currentSource;
        public Source CurrentSource
        {
            get
            {
                if (_currentSource == null)
                {
                    if (JED.JobProperties.Contains(Constants.US_SOURCE_ID))
                    {
                        _currentSource = SetupService.GetById<Source>(SourceId);
                    }
                }
                return _currentSource;
            }
            set
            {
                _currentSource = value;
            }
        }

        private Integration _currentIntegration;

        protected Integration CurrentIntegration
        {
            get
            {
                if (_currentIntegration == null)
                {
                    if (JED.JobProperties.Contains(Constants.US_INTEGRATION_ID))
                    {
                        _currentIntegration = SetupService.GetById<Integration>(IntegrationId);
                    }
                }
                return _currentIntegration;
            }
            set
            {
                _currentIntegration = value;
            }
        }

        protected static Credential GreenhouseS3Creds
        {
            get
            {
                return Credential.GetGreenhouseAWSCredential();
            }
        }

        protected OAuthAuthenticator OAuthAuthenticator(Credential credentials = null, string version = null)
        {
            credentials = credentials ?? this.CurrentCredential;

            if (!string.IsNullOrEmpty(version))
            {
                //for OAUTH 1.0
                var oAuthAuthenticator = new OAuthAuthenticator(HttpClientProvider,
                    credentials.CredentialSet.ConsumerKey,
                    credentials.CredentialSet.ConsumerSecret,
                    credentials.CredentialSet.Token,
                    credentials.CredentialSet.TokenSecret,
                    credentials.CredentialSet.SignatureMethod,
                    version
                );
                return oAuthAuthenticator;
            }
            else
            {
                var oAuthAuthenticator = new OAuthAuthenticator(HttpClientProvider,
                    credentials.CredentialSet.Username,
                    credentials.CredentialSet.ClientId,
                    credentials.CredentialSet.ClientSecret,
                    credentials.CredentialSet.RefreshToken
                );
                return oAuthAuthenticator;
            }
        }

        private IEnumerable<SourceFile> _sourceFiles;
        protected IEnumerable<SourceFile> SourceFiles { get { return _sourceFiles; } }

        protected Credential CurrentCredential
        {
            get
            {
                _credentials = _credentials ?? SetupService.GetItems<Credential>(new { IsActive = true });

                return _credentials.SingleOrDefault(c => c.CredentialID == CurrentIntegration.CredentialID);
            }
        }

        protected void Initialize()
        {
            _credentials = SetupService.GetItems<Credential>(new { IsActive = true });
            _sourceFiles = SetupService.GetAll<SourceFile>("GetSourceFileBySource", new KeyValuePair<string, string>("SourceID", SourceId.ToString()));
            S3Protocol = SetupService.GetById<Lookup>(Constants.SPARK_S3_PROTOCOL)?.Value ?? Constants.URI_SCHEME_S3;
        }

        protected RemoteAccessClient GetRemoteAccessClient(Integration integration = null, string profileName = null)
        {
            if (integration == null)
            {
                integration = CurrentIntegration;
            }
            Credential creds = _credentials.SingleOrDefault(c => c.CredentialID == integration.CredentialID);

            //diat-6081. All internal S3 creds should be using AWSProfile.
            var internalS3ID = SetupService.GetById<Lookup>(Greenhouse.Common.Constants.GREENHOUSE_S3_CREDENTIAL_ID)?.Value.Split(',').ToList();
            bool foundID = internalS3ID.Any(val => int.TryParse(val, out int S3ID) && S3ID == creds.CredentialID);
            if (creds != null && foundID)
            {
                Credential s3Creds;
                if ((Constants.CredentialType)creds.CredentialTypeID == Constants.CredentialType.AWS_ASSUMEROLE)
                    s3Creds = Credential.GetGreenhouseAWSAssumeRoleCredential(creds);
                else if (!string.IsNullOrEmpty(profileName))
                    s3Creds = Credential.GetGreenhouseAWSCredentialFromProfile(profileName);
                else
                    s3Creds = GreenhouseS3Creds;
                return new RemoteAccessClient(new Uri(integration.EndpointURI), s3Creds);
            }
            return new RemoteAccessClient(new Uri(integration.EndpointURI), creds);
        }

        protected RemoteAccessClient GetS3RemoteAccessClient()
        {
            return new RemoteAccessClient(GetDestinationFolder(), GreenhouseS3Creds);
        }

        protected static string GetDatedPartition(DateTime dt)
        {
            return string.Format("date={0}", dt.ToString("yyyy-MM-dd"));
        }

        protected static string GetEntityPartition(string entityId)
        {
            return string.Format("entityid={0}", entityId);
        }

        protected static string GetHourPartition(int hour)
        {
            return String.Format("hour={0}", hour);
        }

        protected string RootBucket
        {
            get
            {
                string bucketPrefix = string.Empty;
                switch (Environment)
                {
                    case "LOCALDEV":
                        bucketPrefix = "dev-";
                        break;
                    case "PROD":
                        //append nothing
                        break;
                    //everything else append the environment name
                    default:
                        bucketPrefix = (Environment.ToLower() + "-");
                        break;
                }
                string root = string.Format("{0}{1}", bucketPrefix, Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseRootBucket);
                return root;
            }
        }

        private readonly Dictionary<string, Amazon.S3.IAmazonS3> _clients = new Dictionary<string, Amazon.S3.IAmazonS3>();

        /// <summary>
        /// This method uploads an incoming file to S3. If the file size is greater than 5GB
        /// the file needs to be uploaded to S3 as multipart; direct copy to S3 does not work.
        /// </summary>
        /// <param name="paths">Keeping the destination path format avoids any potential overwrite issues.</param>
        /// <param name="contentLength">Override for incomingFile.Length</param>
        /// <param name="forceCopyToLocal">Files are downloaded to the local server before being uploaded, no matter the file size.
        /// This can be set to true when the credentials of the origin and destination buckets are different</param>
        protected void UploadToS3(IFile incomingFile, S3File destinationFile, string[] paths, long contentLength = 0, bool forceCopyToLocal = false)
        {
            var fileSize = contentLength > 0 ? contentLength : incomingFile.Length;

            if (fileSize > S3File.MAX_PUT_SIZE || forceCopyToLocal)
            {
                S3MultipartUpload(incomingFile, destinationFile, paths);
            }
            else
            {
                incomingFile.CopyTo(destinationFile, true);
                LogMessage(LogLevel.Debug, $"File imported to: {destinationFile.Uri}");
            }
        }

        protected void S3MultipartUpload(IFile incomingFile, S3File destinationFile, string[] paths)
        {
            LogMessage(LogLevel.Debug, $"FileLength for file {destinationFile.Name} is {incomingFile.Length}.");
            Uri tempDestUri = RemoteUri.CombineUri(new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
            FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

            if (!tempDestFile.Directory.Exists)
            {
                tempDestFile.Directory.Create();
            }
            LogMessage(LogLevel.Debug, $"Importing file to file system first: {tempDestUri}");
            incomingFile.CopyTo(tempDestFile, true);
            LogMessage(LogLevel.Debug, $"Moving to S3 : {destinationFile.Uri}");

            TransferUtility tu = GetMultipartTransferUtility(destinationFile.S3Uri.Region.SystemName);
            tu.UploadAsync(tempDestFile.FullName, destinationFile.S3Uri.Bucket, destinationFile.S3Uri.Key).GetAwaiter().GetResult();
            LogMessage(LogLevel.Info, $"TransferUtility S3 URI {destinationFile.Uri} upload complete");
            tempDestFile.Delete();
        }

        protected TransferUtility GetMultipartTransferUtility(string region)
        {
            if (!_clients.ContainsKey(region))
            {
                lock (locker)
                {
                    if (!_clients.ContainsKey(region))
                    {
                        Amazon.S3.AmazonS3Config config = new Amazon.S3.AmazonS3Config();
                        config.Timeout = new TimeSpan(0, 100, 0); //one hundred minutes
                        config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region);
                        config.UseHttp = false;
                        Amazon.Runtime.AWSCredentials awsCredentials =
                            new Amazon.Runtime.BasicAWSCredentials(GreenhouseS3Creds.CredentialSet.AccessKey,
                                GreenhouseS3Creds.CredentialSet.SecretKey);

                        _clients.Add(region, new AmazonS3Client(awsCredentials, config));
                    }
                }
            }
            return new TransferUtility(_clients[region]);
        }

        protected string DefaultJobCacheKey
        {
            get
            {
                string src = CurrentSource == null ? "ALLSOURCES" : CurrentSource.SourceName;
                string integ = CurrentIntegration == null ? "ALLINTEGRATIONS" : string.Format("{0}-{1}", CurrentIntegration.IntegrationID, CurrentIntegration.IntegrationName);
                return string.Format("JOB_{0}_{1}_{2}", JED.ExecutionPath.CurrentStep.SourceJobStepName, src, integ);
            }
        }

        protected bool IsDuplicateSourceJED()
        {
            int jobCount = 0;

            if (CacheStore == null)
            {
                LogMessage(LogLevel.Debug, $"BaseDragoJob.CacheStore is not set. Unable to check if following JED is a duplicate: {this.DefaultJobCacheKey} for source {CurrentSource.SourceName}");
                return false;
            }

            foreach (var key in CacheStore.GetKeys())
            {
                // DefaultJobCacheKey formats the JED as follows: JOB_{SourceJobStepName}_{SourceName}_{IntegrationID}
                if (key.Contains(this.CurrentSource.SourceName) && key.Contains(JED.ExecutionPath.CurrentStep.SourceJobStepName))
                {
                    jobCount++;
                }
            }
            if (jobCount > 1)
            {
                LogMessage(LogLevel.Info, $"Found duplicate JobCacheKey {this.DefaultJobCacheKey} for source {CurrentSource.SourceName}");
                return true;
            }
            return false;
        }

        private List<SourceType> _sourceTypes;
        protected List<SourceType> SourceTypes
        {
            get
            {
                if (_sourceTypes == null)
                {
                    _sourceTypes = JobService.GetAll<SourceType>("GetSourceType").ToList();
                }
                return _sourceTypes;
            }
        }

        protected void DeleteExistingFileGuid(Data.Model.Core.IFileItem existingFile, string folderName)
        {
            var JobGUID = this.JED.JobGUID.ToString();
            string sourceFileName = existingFile.SourceFileName;
            var sourceType = this.SourceTypes.Find(x => x.SourceID.HasValue && x.SourceID == existingFile.SourceID);

            if (sourceType == null)
            {
                LogMessage(LogLevel.Info, $"Cannot delete existing FileGUID: {existingFile.FileGUID} from S3. This source has a null SourceType");
                return;
            }

            Uri s3BaseUri = Greenhouse.Utilities.RemoteUri.GetServiceUri(Greenhouse.Common.Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);
            var countryName = JobService.GetById<Country>(CurrentIntegration.CountryID).CountryName.ToLower();
            string s3Path = $"{this.RootBucket}/{folderName}/country={countryName}/sourcetype={sourceType.SourceTypeName}/datasource={sourceType.DataSourceName}/filetype={sourceFileName}".ToLower();
            Uri dirURi = Greenhouse.Utilities.RemoteUri.CombineUri(s3BaseUri, s3Path);
            var cred = GreenhouseS3Creds;

            var rac = new Greenhouse.Services.RemoteAccess.RemoteAccessClient(dirURi, cred);

            var s3Dirs = rac.WithDirectory().GetDirectories();
            if (!s3Dirs.Any())
            {
                string errMsg = PrefixJobGuid($"Delivery dataload failed on file type {sourceFileName}. This file has already been processed, but cannot find fileguid {existingFile.FileGUID} on s3. Path {s3Path} returned {s3Dirs.Count()} results");
                throw new DirectoryNotFoundException(errMsg);
            }

            List<string> fileguidDir = new List<string>();
            var currentDay = s3Dirs.Select(x => string.Format("{0}/year={1}/month={2}/day={3}"
                                 , x.Uri.AbsoluteUri.TrimEnd('/')
                                 , existingFile.FileDate.Year, existingFile.FileDate.Month.ToString("00"), existingFile.FileDate.Day.ToString("00")));
            var dayMinus1 = s3Dirs.Select(x => string.Format("{0}/year={1}/month={2}/day={3}"
                                  , x.Uri.AbsoluteUri.TrimEnd('/')
                                  , existingFile.FileDate.Year, existingFile.FileDate.AddDays(-1).Month.ToString("00"), existingFile.FileDate.AddDays(-1).Day.ToString("00")));
            var dayPlus1 = s3Dirs.Select(x => string.Format("{0}/year={1}/month={2}/day={3}"
                                 , x.Uri.AbsoluteUri.TrimEnd('/')
                                 , existingFile.FileDate.Year, existingFile.FileDate.AddDays(1).Month.ToString("00"), existingFile.FileDate.AddDays(1).Day.ToString("00")));
            var fileDateHourPaths = currentDay.Concat(dayMinus1).Concat(dayPlus1);

            foreach (var fileDateHourPath in fileDateHourPaths)
            {
                var fileDateHourDir = rac.WithDirectory(new Uri(fileDateHourPath)).GetDirectories();
                //requires two steps. One to list all hours which is null, and then build the fileguid path
                var fileHour = fileDateHourDir.Select(x => string.Format("{0}/fileguid={1}"
                             , x.Uri.AbsoluteUri.TrimEnd('/')
                             , existingFile.FileGUID)).ToList();
                fileguidDir.AddRange(fileHour);
            }

            //delete exisiting fileguid
            fileguidDir.ForEach(dir =>
            {
                var bucket = rac.WithDirectory(new Uri(dir));
                if (bucket.Exists)
                {
                    LogMessage(LogLevel.Info, $"Deleting directory queueID {existingFile.ID}. Path {bucket.Uri.AbsolutePath} for file guid: {existingFile.FileGUID}");
                    bucket.Delete(true);
                }
            });
        }

        protected string GetS3PathHelper(string entityID, DateTime fileDate, string fileName = "")
        {
            var basePath = GetDestinationFolder();
            string[] paths = new string[] { entityID.ToLower(), GetDatedPartition(fileDate), fileName };
            var srcFileUri = RemoteUri.CombineUri(basePath, paths).ToString();
            return srcFileUri;
        }

        protected void UpdateQueueWithDelete(IEnumerable<IFileItem> queueToDelete, Constants.JobStatus status, bool deleteQueueItem = false)
        {
            foreach (var queueItem in queueToDelete)
            {
                JobService.UpdateQueueStatus(queueItem.ID, status);
                if (deleteQueueItem)
                {
                    LogMessage(LogLevel.Info, $"Deleting queueID {queueItem.ID}. Queue Status: {status}");
                    JobService.DeleteQueue(queueItem.ID);
                }
            }
        }

        protected static void UpdateQueueStatus(IEnumerable<IFileItem> queueToDelete, Constants.JobStatus status)
        {
            foreach (var queueItem in queueToDelete)
            {
                JobService.UpdateQueueStatus(queueItem.ID, status);
            }
        }

        #region Spark

        protected Policy GetPollyPolicy<T>(string fileGUID, IBackOffStrategy backOff) where T : Exception
        {
            var policy = Policy.Handle<T>()
                .Or<Exception>()
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => backOff.GetNextTime(),
                (exception, timeSpan, retryCount, context) =>
                {
                    string methodName = "<NOT SPECIFIED>";
                    if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                    LogException(LogLevel.Warn, string.Format("{3}-{4}.Job error from method: {0} with Exception: {1}. Backoff Policy retry attempt: {2}", methodName, exception.Message, retryCount, JED.JobGUID.ToString(), fileGUID), exception);
                }
              );

            return policy;
        }

        protected Policy GetPollyPolicyWebEx<T>(string fileGUID, IBackOffStrategy backOff, IEnumerable<int> httpStatusNoRetry) where T : Exception
        {
            var policy = Policy.Handle<T>()
                .Or<Exception>()
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => backOff.GetNextTime(),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        if (exception is WebException)
                        {
                            if (exception.InnerException != null)
                            {
                                var webEx = exception.InnerException as WebException;
                                if (webEx != null)
                                {
                                    var response = webEx.Response as HttpWebResponse;
                                    if (response != null)
                                    {
                                        int statusCode = (int)response.StatusCode;
                                        if (httpStatusNoRetry.Contains(statusCode))
                                        {
                                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;
                                            LogException(LogLevel.Error,
                                                $"{JED.JobGUID.ToString()}-{fileGUID}. STOP POLLY RETRY - Response status Code={statusCode} - Job error from method: {context["methodName"]} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception);

                                            context["WebReponseStatusCode"] = statusCode;
                                            cts.Cancel();
                                        }
                                    }
                                }
                            }
                            else
                            {
                                var webEx = exception as WebException;
                                if (webEx?.Response is HttpWebResponse httpWebResponse)
                                {
                                    int statusCode = (int)httpWebResponse.StatusCode;
                                    if (httpStatusNoRetry.Contains(statusCode))
                                    {
                                        var cts = context["CancellationTokenSource"] as CancellationTokenSource;
                                        LogException(LogLevel.Error,
                                            $"{JED.JobGUID.ToString()}-{fileGUID}. STOP POLLY RETRY - Response status Code={statusCode} - Job error from method: {context["methodName"]} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception);

                                        context["WebReponseStatusCode"] = statusCode;
                                        cts.Cancel();
                                    }
                                }
                            }
                        }

                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        LogException(LogLevel.Warn, string.Format("{3}-{4}.Job error from method: {0} with Exception: {1}. Backoff Policy retry attempt: {2}", context["methodName"], exception.Message, retryCount, JED.JobGUID.ToString(), fileGUID), exception);
                    }
                );

            return policy;
        }

        protected Policy GetCancellablePollyPolicy<T>(string fileGUID, IBackOffStrategy backOff, IEnumerable<string> knownExceptions) where T : Exception
        {
            var policy = Policy.Handle<T>()
                .Or<Exception>()
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => backOff.GetNextTime(),
                    (exception, retryCount, context) =>
                    {
                        var matches = knownExceptions.Where(m => exception.Message.Contains(m));

                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (matches.Any())
                        {
                            // the exception is known, poly is cancelled 

                            var cts = context["CancellationTokenSource"] as CancellationTokenSource;
                            string matchingException = string.Join("|", matches);
                            LogException(LogLevel.Error, $"{JED.JobGUID.ToString()}-{fileGUID}. STOP POLLY RETRY - Exception Message(s) matching: {matchingException} - Job error from method: {methodName} with Exception: {exception.Message}. Backoff Policy retry attempt: {retryCount}", exception);
                            context["ExceptionMessages"] = matchingException;
                            cts.Cancel();
                        }
                        else
                        {
                            // Exception thrown not known, will retry
                            LogException(LogLevel.Warn,
                                string.Format(
                                    "{3}-{4}.Job error from method: {0} with Exception: {1}. Backoff Policy retry attempt: {2}",
                                    methodName, exception.Message, retryCount, JED.JobGUID.ToString(),
                                    fileGUID), exception);
                        }
                    }
                );

            return policy;
        }

        /// <summary>
        /// Polly policy that handles T:Exception or Func that returns a boolean value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="fileGUID"></param>
        /// <param name="backOff"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        protected Policy<U> GetPollyPolicy<T, U>(string fileGUID, IBackOffStrategy backOff, Func<U, bool> condition) where T : Exception
        {
            var policy = Policy.Handle<T>()
                .Or<Exception>()
                .OrResult(condition)
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => backOff.GetNextTime(),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (exception?.Exception == null)
                        {
                            LogMessage(LogLevel.Warn, $"{JED.JobGUID.ToString()}-{fileGUID}.Polly policy - Unmet Condition from method: {methodName}- Backoff Policy retry attempt: {retryCount}");
                        }
                        else
                        {
                            LogMessage(LogLevel.Warn, $"{JED.JobGUID.ToString()}-{fileGUID}.Job error from method: {methodName} with Exception: {exception}. Backoff Policy retry attempt: {retryCount}");
                        }
                    }
                );

            return policy;
        }

        /// <summary>
        /// A Polly policy that retries based on a boolean return value
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="fileGUID"></param>
        /// <param name="backOff"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        protected Policy<U> GetPollyRetryPolicy<U>(string fileGUID, IBackOffStrategy backOff, Func<U, bool> condition)
        {
            var policy = Policy.HandleResult<U>(condition)
                .WaitAndRetry(backOff.MaxRetry, retryAttemp => backOff.GetNextTime(),
                    (exception, timeSpan, retryCount, context) =>
                    {
                        //HACK: GetNextTime() increments the counter. We have to decrement it to rest it back to the current counter.
                        backOff.Counter--;

                        string methodName = "<NOT SPECIFIED>";
                        if (context.ContainsKey("methodName")) methodName = context["methodName"].ToString();

                        if (exception?.Exception == null)
                        {
                            LogMessage(LogLevel.Warn, $"{fileGUID}.Polly policy - Unmet Condition from method: {methodName}- Backoff Policy retry attempt: {retryCount}. Thread sleep {backOff.GetNextTime().TotalSeconds}s");
                        }
                        else
                        {
                            LogMessage(LogLevel.Warn, $"{JED.JobGUID.ToString()}-{fileGUID}.Job error from method: {methodName} with Exception: {exception}. Backoff Policy retry attempt: {retryCount}.Thread sleep {backOff.GetNextTime().TotalSeconds}s");
                        }
                    }
                );
            return policy;
        }

        protected T CancellableWebCall<T>(Queue queueItem, Func<T> webCall, IBackOffStrategy backOffStrategy,
            List<int> httpCodeNoRetry, string methodName)
        {
            string logMessage =
                $" - NO POLLY RETRY for queueid={queueItem.ID} entityid={queueItem.EntityID} fileDate={queueItem.FileDate}";
            return CancellableWebCall<T>(webCall, backOffStrategy, httpCodeNoRetry, methodName, queueItem.FileGUID.ToString(), logMessage);
        }

        protected T CancellableWebCall<T>(Func<T> webCall, IBackOffStrategy backOffStrategy, List<int> httpCodeNoRetry, string methodName, string fileGuid, string logMessage = "")
        {
            //providing a cancel token to the context to cancel Polly if certain http codes are returned
            var contextCancelToken = new CancellationTokenSource();
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", contextCancelToken },
                { "methodName", methodName }
            };

            var getPolicy =
                GetPollyPolicyWebEx<Exception>(fileGuid, backOffStrategy, httpCodeNoRetry);
            var policyReturn = getPolicy.ExecuteAndCapture(
                (ctx, ct) => webCall(), policyContext,
                contextCancelToken.Token);

            if (policyReturn.Outcome != OutcomeType.Successful)
            {
                if (policyContext.ContainsKey("WebReponseStatusCode"))
                {
                    var code = (int)policyContext["WebReponseStatusCode"];
                    throw new APIResponseException($"ERROR - API call returned code {code} in {methodName} {logMessage}");
                }

                throw policyReturn.FinalException;
            }

            return policyReturn.Result;
        }

        /// <summary>
        /// Executes a cancellable Poly on the parameter logic. If an exception thrown is listed in the parameter knownExceptions,
        /// the polly is cancelled and the parameter actionOnExceptionMatched is executed
        /// </summary>
        protected T CancellableExecution<T>(Func<T> logic, IBackOffStrategy backOffStrategy, List<string> knownExceptions, string methodName, string fileGuid
            , Action<Context> actionOnExceptionMatched = null, Policy substitutePolicy = null)
        {
            //providing a cancel token to the context to cancel Polly if certain http codes are returned
            var contextCancelToken = new CancellationTokenSource();
            var policyContext = new Context("RetryContext")
            {
                { "CancellationTokenSource", contextCancelToken },
                { "methodName", methodName },
                { "ExceptionMessages", null}
            };

            var getPolicy = substitutePolicy ??
                GetCancellablePollyPolicy<Exception>(fileGuid, backOffStrategy, knownExceptions);
            var policyReturn = getPolicy.ExecuteAndCapture(
                (ctx, ct) => logic(), policyContext,
                contextCancelToken.Token);

            if (policyReturn.Outcome != OutcomeType.Successful)
            {
                if (policyContext["ExceptionMessages"] != null)
                {
                    actionOnExceptionMatched?.Invoke(policyContext);
                }
                else
                {
                    // the exceptions thrown dont match the one(s) expected
                    throw policyReturn.FinalException;
                }
            }

            return policyReturn.Result;
        }

        protected async Task<ResultState> SubmitSparkJobDatabricks(string jobID, Data.Model.Core.IFileItem queueItem, bool isFirstQueueItem, bool updateQueueItem, bool disableIntegrationOverride, params string[] jobParams)
        {
            string guid = queueItem.FileGUID.ToString();
            string jobRunID = null;

            Utilities.BackOffStrategy backOff = new Utilities.BackOffStrategy()
            {
                Counter = 1,
                MaxRetry = 100
            };

            var dataBricksPolicy = GetPollyPolicy<Exception>(guid, backOff);
            var dbPolicy = GetPollyPolicy<Exception>(guid, new Utilities.BackOffStrategy()
            {
                Counter = 1,
                MaxRetry = 10
            });

            var stepDictionary = new Dictionary<string, object>() {
                {
                    "methodName","GetDatabricksJobStatus"
                }
            };

            var dbDictionary = new Dictionary<string, object>() {
                {
                    "methodName","DBUpdate"
                }
            };

            try
            {
                var databricksAPI = SetupService.GetById<Lookup>(Greenhouse.Common.Constants.DATABRICKS_API_CREDS);
                if (string.IsNullOrEmpty(databricksAPI?.Value))
                {
                    throw new LookupException(PrefixJobGuid($"Lookup value for {Greenhouse.Common.Constants.DATABRICKS_API_CREDS} is not defined"));
                }

                var databricksCredential = new Credential(databricksAPI.Value);

                int pageSize;
                if (!int.TryParse(SetupService.GetById<Lookup>(Greenhouse.Common.Constants.DATABRICKS_API_PAGESIZE)?.Value,
                        out pageSize))
                {
                    pageSize = 25;
                }

                #region setup parameters

                var doneSteps = new List<ResultState> {
                    ResultState.SUCCESS,
                    ResultState.FAILED,
                    ResultState.CANCELED
                };

                var jobStatus = ResultState.WAITING;

                bool done = false;

                var sleepTime = backOff.GetNextTime();

                var databricksCalls = new DatabricksCalls
                (
                    databricksCredential, pageSize, HttpClientProvider
                );
                #endregion

                //If first item on the queue, check if the job has run already
                if (isFirstQueueItem)
                {
                    var latestRun = dataBricksPolicy.Execute((_) => databricksCalls.GetLatestJobRun(PrefixJobGuid, jobID, guid), stepDictionary);

                    if (latestRun != null)
                    {
                        //allow Integration Override to re-submit the previously failed spark job
                        var failedStepStates = doneSteps.Where(x => x != ResultState.SUCCESS);

                        if (CurrentIntegration.IsOverrideFailure && failedStepStates.Any(x => x == latestRun.State.ResultStateEnum))
                        {
                            LogMessage(NLog.LogLevel.Info, $"FileGUID: {guid}; Is first item in Queue; previously ran with failure. Summary: {Newtonsoft.Json.JsonConvert.SerializeObject(latestRun)}");
                            //reset IsOverrideFailure
                            CurrentIntegration.IsOverrideFailure = false;
                            CurrentIntegration.LastUpdated = DateTime.Now;
                            dbPolicy.Execute((_) => JobService.Update(CurrentIntegration), dbDictionary);
                            jobRunID = null;//let s resubmit the job
                        }
                        else
                        {
                            LogMessage(NLog.LogLevel.Info, $"FileGUID: {guid}; Is first item in Queue; previously ran with Summary: {Newtonsoft.Json.JsonConvert.SerializeObject(latestRun)}");
                            done = doneSteps.Any(x => x == latestRun.State.ResultStateEnum);
                            jobStatus = latestRun.State.ResultStateEnum;
                            jobRunID = latestRun.JobRunID.ToString();
                        }
                    }
                }

                #region Submit spark job step

                if (jobRunID == null)
                {
                    // adding the fileguid as the first parameter to help identify the jobrun
                    string[] fullJobParams = (new[] { "FileGUID=" + guid }).Concat(jobParams).ToArray();

                    var cmd = new DAL.Databricks.JobRunRequest()
                    {
                        JobID = Convert.ToInt64(jobID),
                        JarParams = fullJobParams
                    };

                    var response = dataBricksPolicy.Execute((_) =>
                        databricksCalls.RunJob(cmd), new Dictionary<string, object>()
                    {
                        {
                            "methodName", "Submit Databricks Job"
                        }
                    });

                    jobRunID = response.RunID.ToString();

                    LogMessage(NLog.LogLevel.Info, $"FileGUID: {guid}; Spark job JobRun - JobRun ID: {jobRunID};");
                }

                #endregion

                LogMessage(NLog.LogLevel.Info, $"FileGUID: {guid}; Update queue status to 'Running'; jobRunID: {jobRunID}");

                var fallBackPolicy = GetFallBackPolicy(guid, jobRunID);
                Policy.Wrap(fallBackPolicy, dbPolicy)
                    .Execute((_) => JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running), dbDictionary);

                //keep checking until job is complete
                while (!done)
                {
                    LogMessage(LogLevel.Info, $"FileGUID: {guid}; calling  databricksCalls.GetJobStatus ; jobRunID: {jobRunID}");
                    jobStatus = GetJobStatus(dataBricksPolicy, databricksCalls, jobRunID, stepDictionary);
                    done = doneSteps.Any(x => x == jobStatus);

                    if (!done)
                    {
                        sleepTime = backOff.GetNextTime();
                        LogMessage(NLog.LogLevel.Info, $"FileGUID: {guid}; Spark job ({jobRunID}) not complete. Putting thread to sleep for :{sleepTime} seconds; status check count: {backOff.Counter}; Databricks CommandID: {jobRunID}");
                        await Task.Delay(sleepTime);
                    }
                }//end while                                

                if (!updateQueueItem)
                    return jobStatus;

                if (jobStatus == ResultState.SUCCESS)
                {
                    LogMessage(NLog.LogLevel.Info, $"SUCCESS->FileGUID: {guid}; Spark job completed - JobRunID={jobRunID ?? "'Not set'"} - JobID={jobID}. Status checked {backOff.Counter} times; Job status: {jobStatus.ToString()}");
                    Policy.Wrap(fallBackPolicy, dbPolicy)
                        .Execute((_) => this.UpdateQueueWithDelete(new List<IFileItem> { queueItem }, Constants.JobStatus.Complete, true), dbDictionary);
                }
                else
                {
                    LogMessage(NLog.LogLevel.Info, $"ERROR->FileGUID: {guid}; Spark job completed - JobRunID={jobRunID ?? "'Not set'"} - JobID={jobID}. Status checked {backOff.Counter} times; Job status: {jobStatus.ToString()}");
                    Policy.Wrap(fallBackPolicy, dbPolicy)
                        .Execute((_) => JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error), dbDictionary);

                    LogMessage(NLog.LogLevel.Info, $"ERROR->FileGUID: {guid};deleting from Partition table fileguid entries.");
                    Policy.Wrap(fallBackPolicy, dbPolicy)
                        .Execute((_) => JobService.DeletePartitionByFileGUID(guid), dbDictionary);
                }

                return jobStatus;
            }
            catch (Exception)
            {
                var excFallBackPolicy = GetFallBackPolicy(guid, jobRunID.ToString());

                if (!disableIntegrationOverride)
                {
                    //TODO: Deactivate integration. Send e-mail with title: "Catastrophic spark job failure"
                    LogMessage(NLog.LogLevel.Error, $"Exception->FileGUID: {guid}; Spark job failed  - JobRunID={jobRunID ?? "'Not set'"} - JobID={jobID} or a Databricks exception has occurred. QueueID {queueItem.ID}; Integration: {_currentIntegration.IntegrationID}. SourceFileName : {queueItem.SourceFileName}. Going disable integration");

                    Policy.Wrap(excFallBackPolicy, dbPolicy)
                      .Execute((_) =>
                      {
                          CurrentIntegration.IsActive = false;
                          JobService.Update(CurrentIntegration);
                      }, dbDictionary);
                }

                Policy.Wrap(excFallBackPolicy, dbPolicy)
                    .Execute((_) => JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error), dbDictionary);

                LogMessage(NLog.LogLevel.Error, $"Exception->FileGUID: {guid};deleting from Partition table fileguid entries.");

                Policy.Wrap(excFallBackPolicy, dbPolicy)
                   .Execute((_) => JobService.DeletePartitionByFileGUID(guid), dbDictionary);

                LogMessage(NLog.LogLevel.Error, $"Exception->FileGUID: {guid};Sending error alert e-mail.");
                SendErrorEmail(guid, jobRunID);

                throw;
            }
        }

        private ResultState GetJobStatus(Policy dataBricksPolicy, DatabricksCalls databricksCalls, string jobRunID,
            Dictionary<string, object> stepDictionary)
        {
            ResultState jobStatus;
            var result = dataBricksPolicy.Execute((_) => databricksCalls.GetJobStatus(jobRunID, PrefixJobGuid), stepDictionary);

            if (result.State.LifeCycleState == "RUNNING")
            {
                jobStatus = ResultState.WAITING;
            }
            else
            {
                jobStatus = result.State.ResultStateEnum;
            }

            return jobStatus;
        }

        /// <summary>
        /// Helper Polly policy to handle when the policy max retry has been exhausted and the action still fails
        /// </summary>
        /// <param name="fileGUID"></param>
        /// <param name="emrStepID"></param>
        /// <returns></returns>
        private Policy GetFallBackPolicy(string fileGUID, string emrStepID)
        {
            Policy fallbackPolicy = Policy
                  .Handle<Exception>()
                  .Fallback(() => SendDBUpdateErrorEmail(fileGUID, emrStepID));
            return fallbackPolicy;
        }

        private void SendDBUpdateErrorEmail(string fileGUID, string emrStepID)
        {
            var jobGUID = JED.JobGUID.ToString();
            var _emailList = SetupService.GetById<Lookup>(Greenhouse.Common.Constants.JOB_SPARK_ERROR_RECIPIENTS);
            if (_emailList == null || string.IsNullOrEmpty(_emailList.Value))
            {
                LogMessage(LogLevel.Info, string.Format("{0}-{1}. Unable to send Spark Job error e-mail. No recipients found from Lookup key: {2}", fileGUID, jobGUID, Greenhouse.Common.Constants.JOB_SPARK_ERROR_RECIPIENTS));
                return;
            }

            var mailMessage = GetMailMessage("Spark Job -SQL DB Update Failure");
            mailMessage.To.Add(_emailList.Value);
            var msg = new StringBuilder();
            msg.Append("<p>During Spark Job, there was a SQL timeout or an error has occurred while trying to perform update on Queue.<br/>");
            msg.Append("<p>The DBUpdate Polly Policy has reached its max retries.<br/>");
            msg.AppendFormat("IntegrationID: {0}<br/>", CurrentIntegration.IntegrationID);
            msg.AppendFormat("Integration Name: {0}<br/>", CurrentIntegration.IntegrationName);
            msg.AppendFormat("EMR Step ID: {0}<br/>", emrStepID);
            msg.AppendFormat("File GUID: {0}<br/>", fileGUID);
            msg.AppendFormat("Job GUID: {0}<br/>", jobGUID);
            mailMessage.Body = msg.ToString();

            var mailClient = new Greenhouse.Mail.Clients.SMTPMailClient();
            var mailResult = mailClient.SendMessage(mailMessage);
            if (mailResult == null || !MailResult.Success)
            {
                LogMessage(LogLevel.Error, $"Error Sending Email in BaseFrameworkJob.SendDBUpdateErrorEmail - Exception: {mailResult?.Error?.Message} - Recipient(s): {_emailList?.Value} - Email Body: {msg}");
            }

            LogMessage(LogLevel.Info, string.Format("{0}-{1}. Spark Job -SQL DB Update Failure e-mail sent to {2}", fileGUID, jobGUID, _emailList.Value));
        }

        private void SendErrorEmail(string fileGUID, string emrStepID)
        {
            var jobGUID = JED.JobGUID.ToString();
            var _emailList = SetupService.GetById<Lookup>(Greenhouse.Common.Constants.JOB_SPARK_ERROR_RECIPIENTS);
            if (_emailList == null || string.IsNullOrEmpty(_emailList.Value))
            {
                LogMessage(LogLevel.Info, string.Format("{0}-{1}. Unable to send Spark Job error e-mail. No recipients found from Lookup key: {2}", fileGUID, jobGUID, Greenhouse.Common.Constants.JOB_SPARK_ERROR_RECIPIENTS));
                return;
            }

            var mailMessage = GetMailMessage("Spark Job Error");
            mailMessage.To.Add(_emailList.Value);
            var msg = new StringBuilder();
            msg.Append("<p>There was a catastrophic Spark Job failure. The integration has been disabled and will require manual intervention.<br/>");
            msg.AppendFormat("IntegrationID: {0}<br/>", CurrentIntegration.IntegrationID);
            msg.AppendFormat("Integration Name: {0}<br/>", CurrentIntegration.IntegrationName);
            msg.AppendFormat("EMR Step ID: {0}<br/>", emrStepID);
            msg.AppendFormat("File GUID: {0}<br/>", fileGUID);
            msg.AppendFormat("Job GUID: {0}<br/>", jobGUID);
            mailMessage.Body = msg.ToString();

            var mailClient = new Greenhouse.Mail.Clients.SMTPMailClient();
            var mailResult = mailClient.SendMessage(mailMessage);
            if (mailResult == null || !MailResult.Success)
            {
                LogMessage(LogLevel.Error, $"Error Sending Email in BaseFramework.SendErrorEmail - Exception: {mailResult?.Error?.Message} - Recipient(s): {_emailList?.Value} - Email Body: {msg}");
            }
            LogMessage(LogLevel.Info, string.Format("{0}-{1}. Spark Job error e-mail sent to {2}", fileGUID, jobGUID, _emailList.Value));
        }

        private MailMessage GetMailMessage(string subject)
        {
            //Build mail _mailMessage
            var mailMessage = new System.Net.Mail.MailMessage();
            mailMessage.IsBodyHtml = true;
            mailMessage.Sender = new MailAddress(Greenhouse.Configuration.Settings.Current.Email.MailAdminFrom, "DataLake Admin");
            mailMessage.From = new MailAddress(Greenhouse.Configuration.Settings.Current.Email.MailAdminFrom, "DataLake Admin");
            string env = (base.Environment == "PROD" ? string.Empty : string.Format("{0}-", base.Environment.ToUpper()));
            mailMessage.Subject = string.Format("{0}{1}", env, subject);
            mailMessage.Priority = MailPriority.High;

            return mailMessage;
        }

        #endregion

        /// <summary>
        /// Appends JobGuid to the beginning of the param [message]
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        protected string PrefixJobGuid(string message)
        {
            return
                $"{this.JED.JobGUID.ToString()} - " +
                message;
        }

        #region Aggregate job specific
        protected IEnumerable<System.Data.Odbc.OdbcParameter> GetScriptParameters(string stagefilepath, string fileGuid,
            string fileDate = null, string manifestFilePath = null, string entityId = null,
            string fileCollection = null, string compressionOption = null,
            string profileid = null, Dictionary<string, string> overrideDictionary = null, string profileName = null, string apiEntityName = null)
        {
            List<System.Data.Odbc.OdbcParameter> parameters = new List<System.Data.Odbc.OdbcParameter>();
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "stagefilepath", Value = stagefilepath });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "accesskey", Value = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().AccessKey });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "secretkey", Value = Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().SecretKey });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "region", Value = Greenhouse.Configuration.Settings.Current.AWS.Region });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "iamrole", Value = Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3 });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "fileguid", Value = fileGuid });
            parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "rootbucket", Value = RootBucket });

            if (!string.IsNullOrEmpty(fileDate))
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "filedate", Value = fileDate });

            if (!string.IsNullOrEmpty(manifestFilePath))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "manifest", Value = manifestFilePath });
            }

            if (!string.IsNullOrEmpty(entityId))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "entityid", Value = entityId });
            }

            if (!string.IsNullOrEmpty(apiEntityName))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "entityname", Value = apiEntityName });
            }

            if (!string.IsNullOrEmpty(profileid))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "profileid", Value = profileid });
            }

            if (!string.IsNullOrEmpty(profileName))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "profilename", Value = profileName });
            }

            if (!string.IsNullOrEmpty(fileCollection))
            {
                var stageFileDictionary = ETLProvider.GetStageFileDictionary(fileCollection);
                foreach (var keyValuePair in stageFileDictionary)
                {
                    parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = $"sourcefile-{keyValuePair.Key}", Value = keyValuePair.Value });
                }
            }

            if (compressionOption != null)
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = "compression", Value = compressionOption });
            }

            if (overrideDictionary != null)
            {
                foreach (var keyValuePair in overrideDictionary)
                {
                    //skip adding parameters that are configured already, eg @compression
                    if (parameters.Any(x => x.ParameterName.Contains(keyValuePair.Key))) continue;
                    parameters.Add(new System.Data.Odbc.OdbcParameter() { ParameterName = $"{keyValuePair.Key}", Value = keyValuePair.Value });
                }
            }

            var parameterDescription = SanitizeAWSCredentials(TraceParameters(parameters));
            LogMessage(LogLevel.Info, $"Script will execute with the following parameters: {parameterDescription}");

            return parameters;
        }

        private static string TraceParameters(IEnumerable<System.Data.Odbc.OdbcParameter> parameters)
        {
            StringBuilder sb = new StringBuilder();
            foreach (System.Data.Odbc.OdbcParameter p in parameters)
            {
                sb.Append(string.Format("{0} = {1}, ", p.ParameterName, p.Value));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Delete staged files and directory.
        /// </summary>
        /// <param name="paths"></param>
        /// <param name="FileGUID"></param>
        protected void DeleteStageFiles(string[] paths, Guid FileGUID)
        {
            var rac = GetS3RemoteAccessClient();
            var uri = GetUri(paths, Constants.ProcessingStage.STAGE);
            var dir = rac.WithDirectory(uri);
            if (dir.Exists)
            {
                LogMessage(LogLevel.Info, $"Start deleting stage path - {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName}; FileGuid: {FileGUID}");
                dir.Delete(true);
                LogMessage(LogLevel.Info, $"End deleting stage path - {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName}; FileGuid: {FileGUID}");
            }
        }

        /// <summary>
        /// Delete stage files that contains match in their name
        /// </summary>
        /// <param name="paths">path to delete file from</param>
        /// <param name="match">eventual match pattern to delete a selection of files. Delete of files if not specified</param>
        protected void DeleteStageFiles(string[] paths, Guid FileGUID, string match = null)
        {
            var rac = GetS3RemoteAccessClient();
            var uri = GetUri(paths, Constants.ProcessingStage.STAGE);
            var dir = rac.WithDirectory(uri);
            if (!dir.Exists)
                return;

            var files = dir.GetFiles();
            var matchingFiles = string.IsNullOrEmpty(match) ? files : files.Where(f => f.Name.Contains(match));
            var nbFiles = matchingFiles.Count();

            LogMessage(LogLevel.Debug, $"Start deleting {nbFiles} Files matching '{match}' in folder {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName}; FileGuid: {FileGUID}");
            matchingFiles.ToList().ForEach(m => m.Delete());
            LogMessage(LogLevel.Debug, $"End deleting {nbFiles} Files matching '{match}' in folder  {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName}; FileGuid: {FileGUID}");

            if (!dir.GetFiles().Any())
            {
                LogMessage(LogLevel.Debug, $"Directory is empty. Deleting folder {dir.ToString()}; FileGuid: {FileGUID}");
                dir.Delete(true);
            }
        }

        /// <summary>
        /// Delete raw files that contains match in their name
        /// </summary>
        /// <param name="paths">path to delete file from</param>
        /// <param name="match">eventual match pattern to delete a selection of files. Delete of files if not specified</param>
        protected void DeleteRawFiles(string[] paths, string match = null)
        {
            var rac = GetS3RemoteAccessClient();
            var uri = GetUri(paths, Constants.ProcessingStage.RAW);
            var dir = rac.WithDirectory(uri);
            var files = dir.GetFiles();
            var matchingFiles = string.IsNullOrEmpty(match) ? files : files.Where(f => f.Name.Contains(match));
            var nbFiles = matchingFiles.Count();

            LogMessage(LogLevel.Info, $"Start deleting {nbFiles} Files matching '{match}' in folder {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName};");
            matchingFiles.ToList().ForEach(m => m.Delete());
            LogMessage(LogLevel.Info, $"End deleting {nbFiles} Files matching '{match}' in folder  {dir.ToString()}; Integration: {CurrentIntegration.IntegrationName};");
        }

        /// <summary>
        /// Delete raw files created previously for a queue (using its fileguid)
        /// </summary>
        /// <param name="queue">using the queue EntityID, FileDate and FileGUID to locate and delete the raw files</param>
        protected void DeleteRawFiles(Queue queue)
        {
            string[] path = { queue.EntityID.ToLower(), GetDatedPartition(queue.FileDate) };
            DeleteRawFiles(path, queue.FileGUID.ToString());
        }

        protected Uri GetUri(string[] paths, Constants.ProcessingStage stage)
        {
            var prevStage = Stage;
            Stage = stage;

            var baseDestUri = GetDestinationFolder();
            if (paths?.Length == 0)
                return baseDestUri;

            Uri destUri = RemoteUri.CombineUri(baseDestUri, paths);
            Stage = prevStage;
            return destUri;
        }

        protected Uri GetRedshiftSpectrumFolder()
        {
            Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3,
                Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);
            var spectrumFolderName = SetupService.GetById<Lookup>(Common.Constants.REDSHIFT_SPECTRUM_S3_FOLDER).Value;
            string[] paths = new string[]
            {
                spectrumFolderName.ToLower(),
                CurrentSource.SourceName.Replace(" ", string.Empty).ToLower()
            };
            return RemoteUri.CombineUri(baseUri, paths);
        }
        #endregion

        public static string GenerateDateExpression(DateTime date)
        {
            List<string> dateExpressionComponents = new List<string>
            {
                //prepend the interval enum so it will match the full regex pattern (except for "LASTDAYOFTHEMONTH")
                //examples: "EveryMonday", "Every15", "FirstSaturday"
                $"{AggregateInitializeSettings.IntervalEnum.Every}{date.DayOfWeek.ToString().ToUpper()}",
                $"{AggregateInitializeSettings.IntervalEnum.Every}{date.ToString("dd")}"
            };

            if (UtilsDate.IsFirstDayOfMonth(date, date.DayOfWeek))
            {
                dateExpressionComponents.Add($"{AggregateInitializeSettings.IntervalEnum.First}{date.DayOfWeek.ToString().ToUpper()}");
            }

            if (UtilsDate.IsLastDayOfMonth(date, date.DayOfWeek))
            {
                dateExpressionComponents.Add($"{AggregateInitializeSettings.IntervalEnum.Last}{date.DayOfWeek.ToString().ToUpper()}");
            }

            if (UtilsDate.IsLastDayOfMonth(date))
            {
                dateExpressionComponents.Add($"{AggregateInitializeSettings.IntervalEnum.LastDayOfTheMonth}");
            }

            string dateExpression = string.Join(" ", dateExpressionComponents);
            return dateExpression;
        }

        protected bool IsTrueUpScheduled(DateTime importDate, ICollection<AggregateInitializeSettings.TrueUp> trueUpDetails)
        {
            var isScheduled = false;
            var dateExpression = GenerateDateExpression(importDate);

            foreach (var trueUp in trueUpDetails.OrderByDescending(t => t.Priority))
            {
                if (trueUp.Interval == AggregateInitializeSettings.IntervalEnum.Daily)
                {
                    isScheduled = true;
                    break;
                }

                //the "triggerDayRegex" can be either "1" or "Monday" as in "schedule job to run on the 1st" or "schedule for Monday"
                var triggerDayRegex = trueUp.TriggerDayRegex;
                int day = 1;
                var triggerIsNumber = int.TryParse(trueUp.TriggerDayRegex, out day);
                if (triggerIsNumber)
                {
                    triggerDayRegex = trueUp.TriggerDayRegex.PadLeft(2, '0');
                }

                var trueUpRegex = (trueUp.Interval == AggregateInitializeSettings.IntervalEnum.LastDayOfTheMonth) ? $"{trueUp.Interval}" : $"{trueUp.Interval}{triggerDayRegex}";

                if (Regex.IsMatch(dateExpression, trueUpRegex, RegexOptions.IgnoreCase))
                {
                    LogMessage(LogLevel.Debug, $"Matching schedule found->DateExpression:{dateExpression}->TrueUpRegex:{trueUpRegex}.");
                    isScheduled = true;
                    break;
                }
            }

            return isScheduled;
        }

        protected static Credential GetGreenhouseS3CredsWithProfile(string profileName)
        {
            return Credential.GetGreenhouseAWSCredential(profileName);
        }

        /// <summary>
        /// Removing sensitive information from a text
        /// </summary>
        /// <param name="text">text to sanitize</param>
        /// <param name="maxLength">shorten the text to sanitize. Useful when sanitizing a script that has not been converted to a stored procedure. No need to log a long text </param>
        /// <returns></returns>
        public static string SanitizeAWSCredentials(string text, int? maxLength = null)
        {
            string sanitized = text
                .Replace(Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().AccessKey,
                    "<AccessKey:sanitized>")
                .Replace(Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().SecretKey,
                    "<SecretKey:sanitized>")
                .Replace(Greenhouse.Configuration.Settings.Current.AWS.IamRoleRedshiftCopyS3,
                    "<IamRole:sanitized>")
                .Replace(Greenhouse.Configuration.Settings.Current.AWS.Region,
                    "<Region:sanitized>");

            if (maxLength.HasValue)
            {
                return sanitized.Substring(0, maxLength.Value > sanitized.Length ? sanitized.Length : maxLength.Value);
            }

            return sanitized;
        }

        private void LogMessage(LogLevel logLevel, string message)
        {
            logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(message)));
        }

        private void LogException(LogLevel logLevel, string message, Exception exc = null)
        {
            logger.Log(Msg.Create(logLevel, logger.Name, PrefixJobGuid(message), exc));
        }
    }
}

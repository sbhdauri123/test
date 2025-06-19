using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.Aggregate.GWI
{
    [Export("GWI-AggregateDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private const string ETL_SCRIPT_PREFIX = "redshiftload";
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private RemoteAccessClient _RAC;
        private Uri _baseRawDestUri;
        private Uri _baseStageDestUri;
        private string JobGUID => base.JED.JobGUID.ToString();
        private Action<string> _logInfo;
        private Action<string> _logDebug;
        private Action<string> _logError;
        private Action<string, Exception> _logErrorExc;
        private int _exceptionCount;
        private string _standaloneColumns;
        private readonly Stopwatch _runtime = new Stopwatch();
        private TimeSpan _maxRuntime;

        public void PreExecute()
        {
            base.Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            _RAC = GetS3RemoteAccessClient();
            _baseRawDestUri = GetDestinationFolder();
            _baseStageDestUri = new Uri(_baseRawDestUri.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
            base.CurrentIntegration = SetupService.GetById<Integration>(base.IntegrationId);
            // standalone columns that should not be unpivoted
            _standaloneColumns = SetupService.GetById<Lookup>(Constants.GWI_STANDALONE_COLUMNS)?.Value ?? "hash,wave,gwi-usa.country";

            _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));
            _logDebug = (msg) => _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid(msg)));
            _logError = (msg) => LogAndAddError(msg);
            _logErrorExc = (msg, exc) => LogAndAddError(msg, exc);

            _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);

            _logInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");
        }

        public void Execute()
        {
            _logInfo($"EXECUTE START {base.DefaultJobCacheKey}");
            string sparkJobName = "GWIDelivery";

            if (IsDuplicateSourceJED())
                return;

            var queueItems = JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID);

            if (queueItems.Any())
            {
                _runtime.Start();

                foreach (var queueItem in queueItems)
                {
                    if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
                    {
                        _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                            PrefixJobGuid($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job")));
                        break;
                    }

                    try
                    {
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                        SourceFile sf = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(queueItem.FileName));

                        if (sf == null)
                        {
                            _logError($"Filename: {queueItem.FileName} skipped because no matching source file found");
                            continue;
                        }

                        var stageFilePathUri = GetStageFilePath(queueItem, GetDestinationFolder());
                        var fileName = queueItem.FileName;

                        if (sf.SourceFileName == "respondent-level-data")
                        {
                            fileName = PreprocessRespondentFile(sparkJobName, queueItem, sf);
                            stageFilePathUri = GetStageFilePath(queueItem, _baseStageDestUri);
                        }

                        DataLoadFile(stageFilePathUri, queueItem, fileName);

                        //Update and Delete Queue
                        //Using Polly to handle SQL timeout errors
                        PollyAction(() =>
                        {
                            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
                        }, "UpdateQueueWithDelete");

                        _logInfo($"Processing Complete for Queue ID ={queueItem.ID} fileguid={queueItem.FileGUID}");
                    }
                    catch (Exception exc)
                    {
                        _logErrorExc($"Dataload failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                        break;
                    }
                }

                if (_exceptionCount > 0)
                {
                    throw new ErrorsFoundException($"Total errors: {this._exceptionCount}; Please check Splunk for more detail.");
                }
            }//end if queue.Any()
            else
            {
                _logInfo("There are no items in the Queue");
            }
        }

        private static Uri GetStageFilePath(IFileItem queueItem, Uri filePathUri)
        {
            var stagePaths = new string[] { GetDatedPartition(queueItem.FileDate) };
            var stageFilePathUri = RemoteUri.CombineUri(filePathUri, stagePaths);
            return stageFilePathUri;
        }

        private string PreprocessRespondentFile(string sparkJobName, IFileItem queueItem, SourceFile sf)
        {
            var jobParams = new string[]
            {
                S3Protocol
                , this.RootBucket
                , $"{Constants.ProcessingStage.RAW.ToString().ToLower()}/{CurrentSource.SourceName.Replace(" ", string.Empty).ToLower()}/" +
                $"{GetDatedPartition(queueItem.FileDate)}/{queueItem.FileName}"
                , $"{sf.FileDelimiter}"
                , $"{S3Protocol}://{this.RootBucket}/{Constants.ProcessingStage.STAGE.ToString().ToLower()}/{CurrentSource.SourceName.Replace(" ", string.Empty).ToLower()}/" +
                $"{GetDatedPartition(queueItem.FileDate)}/fileguid={queueItem.FileGUID.ToString().ToLower()}"
                , $"{_standaloneColumns}"
                , $"{queueItem.FileGUID.ToString().ToLower()}"
            };

            var msg = string.Format(
                "{3},{4} - Submitting spark job for integration: {0}; source: {1}; with parameters {2}",
                CurrentIntegration.IntegrationID, queueItem.SourceFileName, JsonConvert.SerializeObject(jobParams),
                JobGUID, queueItem.FileGUID);
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, msg));

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

            if (etlJob == null)
            {
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
            }

            // set isFirstQueueItem flag to TRUE always in order to check if job has already been submitted
            // since we are loading data into Redshift, this dataload job will loop through all integrations
            // ie "first-Queue-Item" may change
            var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID,
                queueItem, true, false, false, jobParams));
            job.Wait();

            var jsonResult = JsonConvert.SerializeObject(job.Result);
            //If job failed, then throw exception               
            if (job.Result != ResultState.SUCCESS)
            {
                string errMessage = PrefixJobGuid($"ERROR->Spark job for queue id: {queueItem.ID} returned job status: {job.Result.ToString()}");
                throw new DatabricksResultNotSuccessfulException(errMessage);
            }
            else
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"SUCCESS->Spark job for integration: {CurrentIntegration.IntegrationID};queue id: {queueItem.ID}; source: {queueItem.SourceFileName}; Summary: {job.Result.ToString()}")));
            }

            var outputFileName = $"fileguid={queueItem.FileGUID.ToString().ToLower()}/part";
            return outputFileName;
        }

        private void LogAndAddError(string errorMessage, Exception exception = null)
        {
            if (exception == null)
            {
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(errorMessage)));
            }
            else
            {
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, PrefixJobGuid(errorMessage), exception));
            }
            _exceptionCount++;
        }

        public void PollyAction(Action call, string logName)
        {
            var backoff = new BackOffStrategy()
            {
                Counter = 0,
                MaxRetry = 10
            };

            GetPollyPolicy<Exception>("GWI-AggregateDeliveryDataLoadJob", backoff)
                .Execute((_) => { call(); },
                    new Dictionary<string, object> { { "methodName", logName } });
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
                _RAC?.Dispose();
            }
        }

        ~DataLoadJob()
        {
            Dispose(false);
        }

        #region ETL methods

        private void DataLoadFile(Uri destBucket, IFileItem queueItem, string fileName)
        {
            //prepare bucket path to work with Redshift
            var stageFilePath = System.Net.WebUtility.UrlDecode($"{destBucket.OriginalString.Trim('/')}");
            string fileDate = queueItem.FileDate.ToString("yyyy-MM-dd");
            string fileGuid = queueItem.FileGUID.ToString();
            //default is to use the source name in the etl script name
            var loadEtlScriptName = $"{ETL_SCRIPT_PREFIX}{CurrentSource.SourceName.ToLower()}.sql";
            //script path
            string[] paths = new string[] { "scripts", "etl", "redshift", CurrentSource.SourceName.ToLower(), loadEtlScriptName };
            Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);
            var scriptPath = RemoteUri.CombineUri(baseUri, paths);
            _logDebug($"Loading script file from: {scriptPath}");
            IFile scriptFile = _RAC.WithFile(scriptPath);
            string script = ETLProvider.GetScript(scriptFile);

            // is the file compressed?
            var fileItem = new FileCollectionItem() { SourceFileName = queueItem.SourceFileName, FilePath = fileName };
            var stageFilesJson = JsonConvert.SerializeObject(new List<FileCollectionItem>() { fileItem });
            var compressionOption = ETLProvider.GetCompressionOption(queueItem, stageFilesJson);

            var parameters = GetScriptParameters(stageFilePath, fileGuid, fileDate, null, CurrentIntegration.IntegrationID.ToString(), stageFilesJson, compressionOption).ToList();
            var headerLine = compressionOption == Constants.CompressionType.GZIP.ToString() ? ETLProvider.GetHeaderLineFromGzip(destBucket, fileName, CurrentSource.AggregateProcessingSettings.FileDelimiter) : ETLProvider.GetHeaderLineFromFile(destBucket, fileName, CurrentSource.AggregateProcessingSettings.FileDelimiter);
            if (!string.IsNullOrEmpty(headerLine))
            {
                parameters.Add(new System.Data.Odbc.OdbcParameter("columnlist", headerLine));
            }
            string cmdText = RedshiftRepository.PrepareCommandText(script, parameters);

            _logDebug($"Script: {scriptFile} prepared and ready to execute with parameters: {RemoveS3TokenForLog(TraceParameters(parameters), Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().AccessKey, Greenhouse.Configuration.Settings.Current.AWS.Credentials.GetCredentials().SecretKey)}");

            int retVal = RedshiftRepository.ExecuteRedshiftCommand(cmdText);
            _logInfo($"Script: {scriptFile} executed, result: {retVal}");
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

        #endregion

        #region Remove Token methods

        public static string RemoveS3TokenForLog(string relativeUrl, string accessToken, string secretKey)
        {
            relativeUrl = relativeUrl.Replace(accessToken, "<access token sanitized>")
                .Replace(secretKey, "<secret key sanitized>");
            return relativeUrl;
        }

        #endregion
    }
}

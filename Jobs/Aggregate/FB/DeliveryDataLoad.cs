using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Greenhouse.Jobs.Aggregate.FB
{
    [Export("FB-AggregateDeliveryDataLoadJob", typeof(IDragoJob))]
    public class DeliveryDataLoadJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private const string ETL_SCRIPT_PREFIX = "redshiftload";
        private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private RemoteAccessClient _RAC;
        private Uri _baseRawDestUri;
        private Uri _baseStageDestUri;
        private Action<string> _logInfo;
        private Action<string> _logDebug;
        private Action<string> _logWarn;
        private Action<string, Exception> _logErrorExc;
        private int _exceptionCount;
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
            _logInfo = (msg) => _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(msg)));
            _logDebug = (msg) => _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name, PrefixJobGuid(msg)));
            _logWarn = (msg) => _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(msg)));
            _logErrorExc = (msg, exc) => LogAndAddError(msg, exc);
            _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);

            _logInfo($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}");
        }

        public void Execute()
        {
            _logInfo($"EXECUTE START {base.DefaultJobCacheKey}");

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
                        _logWarn($"Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}. Stopping the Job");
                        break;
                    }

                    string[] stagePaths =
                    {
                        queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate)
                    };

                    try
                    {
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                        Uri stageFilePathUri = RemoteUri.CombineUri(_baseStageDestUri, stagePaths);

                        DataLoadFile(stageFilePathUri, queueItem);

                        //Update and Delete Queue
                        //Using Polly to handle SQL timeout errors
                        PollyAction(() =>
                        {
                            UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);
                        }, "UpdateQueueWithDelete");

                        _logInfo($"Processing Complete for Queue ID ={queueItem.ID} fileguid={queueItem.FileGUID}");

                        try
                        {
                            //leave stage file deletion at tail end of job (ie after record is marked completed)
                            PollyAction(() =>
                            {
                                DeleteStageFiles(stagePaths, queueItem.FileGUID, queueItem.FileGUID.ToString());
                            }, "DeleteStageFiles");
                        }
                        catch (Exception exc)
                        {
                            _logErrorExc($"Error: Deleting stage files {exc.Message} for Entity: {queueItem.EntityID}; Date: {queueItem.FileDate}; FileGuid: {queueItem.FileGUID}", exc);
                        }
                    }
                    catch (Exception exc)
                    {
                        _logErrorExc($"Dataload failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}", exc);
                        JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                    }
                }

                if (_exceptionCount > 0)
                {
                    throw new ErrorsFoundException($"Total errors: {_exceptionCount}; Please check Splunk for more detail.");
                }
            }//end if queue.Any()
            else
            {
                _logInfo("There are no items in the Queue");
            }
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

            GetPollyPolicy<Exception>("FB-AggregateDeliveryDataLoadJob", backoff)
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

        ~DeliveryDataLoadJob()
        {
            Dispose(false);
        }

        #region ETL methods

        private void DataLoadFile(Uri destBucket, IFileItem queueItem)
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
            var parameters = GetScriptParameters(stageFilePath, fileGuid, fileDate, null, queueItem.EntityID, queueItem.FileCollectionJSON);
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

        #endregion`

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

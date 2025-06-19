using Greenhouse.Common;
using Greenhouse.DAL;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.MediaTools
{
    [Export("MediaTools-DeliveryDataLoad", typeof(IDragoJob))]
    public class DeliveryDataLoadJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private const string SCRIPT_NAME = "redshiftloadmt.sql";
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();
        private RemoteAccessClient rac;
        private Uri baseDestUri;
        private Uri scriptPath;
        private string fileGuid;
        private Queue queueItem;
        private string redShiftScript = string.Empty;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            rac = GetS3RemoteAccessClient();
            baseDestUri = GetDestinationFolder();
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }

        public void Execute()
        {
            try
            {
                rac = GetS3RemoteAccessClient();
                IEnumerable<IFileItem> queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(o => o.FileDate);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"{queueItems.Count()} items retreived from queue for Integration: {CurrentIntegration.IntegrationID} - {CurrentIntegration.IntegrationName}")));

                //script path
                string[] redShiftScriptPath = new string[] { "scripts", "etl", "redshift", CurrentSource.SourceName.ToLower(), SCRIPT_NAME };
                Uri baseUri = RemoteUri.GetServiceUri(Constants.URI_SCHEME_S3, Greenhouse.Configuration.Settings.Current.AWS.Region, this.RootBucket);
                scriptPath = RemoteUri.CombineUri(baseUri, redShiftScriptPath);
                IFile scriptFile = rac.WithFile(scriptPath);

                redShiftScript = ETLProvider.GetScript(scriptFile);

                foreach (IFileItem queueItem in queueItems)
                {
                    this.queueItem = (Queue)queueItem;
                    fileGuid = this.queueItem.FileGUID.ToString();
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"Ready to process file: {this.queueItem.FileName}; guid: {fileGuid}; file date: {this.queueItem.FileDate}")));
                    Data.Services.JobService.UpdateQueueStatus(this.queueItem.ID, Constants.JobStatus.Running);

                    #region Dataload 
                    var sourceFile = base.SourceFiles.FirstOrDefault(x => x.SourceFileName.Equals(this.queueItem.SourceFileName, StringComparison.InvariantCultureIgnoreCase));
                    var fileDelimiter = "";
                    if (sourceFile != null)
                    {
                        fileDelimiter = sourceFile.FileDelimiter;
                    }

                    ProcessEtlScript(this.queueItem, fileDelimiter);

                    //all done, delete from the Queue
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Updating status to 'Complete'. Deleting QueueItem:QueueID: {this.queueItem.ID}; FileGuid: {this.queueItem.FileGUID}")));
                    UpdateQueueWithDelete(new[] { this.queueItem }, Constants.JobStatus.Complete, true);
                    #endregion

                }
            }
            catch (Exception exc)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"PROCESSING ERROR - {CurrentIntegration.IntegrationName}; FileGuid: {fileGuid}")));
                //make sure we log the failure to the transfer logs
                if (queueItem != null)
                {
                    Data.Services.JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Error);
                }
                throw;
            }
        }

        private void ProcessEtlScript(Queue queueItem, string fileDelimiter)
        {
            string[] paths = new string[] { this.queueItem.EntityID.ToLower(), GetDatedPartition(this.queueItem.FileDate) };
            var stageFilePath = System.Net.WebUtility.UrlDecode($"{GetUri(paths, Constants.ProcessingStage.RAW).OriginalString.Trim('/')}");
            string s3StageFileName = $"{stageFilePath}/{queueItem.FileName}";
            string databasename = queueItem.FileName.Split('_')[0];
            string dataYear = queueItem.FileName.Split('_')[2];
            string headerLine = ETLProvider.GetHeaderLineFromFile(RemoteUri.CombineUri(this.baseDestUri, paths), queueItem.FileName, fileDelimiter);

            var odbcParams = base.GetScriptParameters(s3StageFileName, fileGuid.ToString(), queueItem.FileDate.ToString("yyyy/MM/dd")).ToList();

            if (!string.IsNullOrEmpty(databasename))
            {
                odbcParams.Add(new System.Data.Odbc.OdbcParameter("databasename", databasename));
            }

            if (!string.IsNullOrEmpty(dataYear))
            {
                odbcParams.Add(new System.Data.Odbc.OdbcParameter("datayear", dataYear));
            }

            if (!string.IsNullOrEmpty(headerLine))
            {
                odbcParams.Add(new System.Data.Odbc.OdbcParameter("columnlist", headerLine));
            }
            string sql = RedshiftRepository.PrepareCommandText(redShiftScript, odbcParams);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Script: {scriptPath} prepared and ready to execute with additional params: databasename => {databasename}; datayear => {dataYear}; columnlist => {headerLine}; for FileGUID: {fileGuid}")));
            int result = RedshiftRepository.ExecuteRedshiftCommand(sql);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Script: {scriptPath} executed, result: {result}")));
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
                rac?.Dispose();
            }
        }

        ~DeliveryDataLoadJob()
        {
            Dispose(false);
        }
    }
}

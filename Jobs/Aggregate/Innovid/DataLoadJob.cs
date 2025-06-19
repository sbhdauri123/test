using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using NLog;
using System;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.Linq;

namespace Greenhouse.Jobs.Aggregate.Innovid
{
    [Export("Innovid-AggregateDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        private string JobLogGUID => this.JED.JobGUID.ToString();
        private readonly Greenhouse.DAL.ETLProvider _ETLProvider = new DAL.ETLProvider();
        private readonly Stopwatch _runtime = new Stopwatch();
        private TimeSpan _maxRuntime;

        public void PreExecute()
        {
            CurrentIntegration = SetupService.GetById<Integration>(base.IntegrationId);
            _ETLProvider.SetJobLogGUID(this.JobLogGUID);
            _maxRuntime = LookupService.GetProcessingMaxRuntime(CurrentSource.SourceID);
        }

        public void Execute()
        {
            if (IsDuplicateSourceJED())
                return;

            var queueItems = JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID);

            //script path
            string[] redShiftScriptPath = new string[] { "scripts", "etl", "redshift", CurrentSource.SourceName.ToLower(), "redshiftloadinnovid.sql" };
            var redshiftSqlScriptPath = string.Join("/", redShiftScriptPath);
            var redshiftProcessSQL = DAL.ETLProvider.GetRedshiftScripts(base.RootBucket, redShiftScriptPath);
            int exceptionCount = 0;
            _runtime.Start();

            foreach (Queue queueItem in queueItems)
            {
                if (TimeSpan.Compare(_runtime.Elapsed, _maxRuntime) == 1)
                {
                    _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name,
                        PrefixJobGuid($"Stopping the Job. Current runtime:{_runtime.Elapsed} greater than maxRuntime:{_maxRuntime}")));
                    break;
                }

                try
                {
                    if (queueItem.FileCollection == null || !queueItem.FileCollection.Any())
                    {
                        var missingFileCollectionException = new NullOrEmptyFileCollectionException(
                            $"{CurrentIntegration.IntegrationName}; FileGuid: {queueItem.FileGUID} - Skipping: No FileCollection Found. Resetting to Import Pending to retry downloading files");
                        exceptionCount++;
                        continue;
                    }

                    string[] paths = new string[] { GetDatedPartition(queueItem.FileDate) };
                    var pathDescription = string.Join("/", paths);

                    //Set destination for stage files
                    var destUri = GetUri(paths, Constants.ProcessingStage.STAGE);
                    var sourceUri = GetUri(paths, Constants.ProcessingStage.RAW);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"Start processing - {CurrentIntegration.IntegrationName}; FileGUid: {queueItem.FileGUID}; path={pathDescription}; script: {redshiftSqlScriptPath}")));

                    queueItem.Step = Constants.ExecutionType.Processing.ToString();
                    queueItem.Status = Constants.JobStatus.Running.ToString();
                    queueItem.StatusId = (int)Constants.JobStatus.Running;
                    JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                    DeleteStageFiles(paths, queueItem.FileGUID);
                    _ETLProvider.StageInnovidFiles(queueItem.FileCollection, sourceUri, destUri);

                    //PROCESS
                    var stageFilePath = $"{destUri.OriginalString.Trim('/')}";
                    var fileDate = queueItem.FileDate.ToString("yyyy-MM-dd");
                    var fileGuid = queueItem.FileGUID.ToString();
                    var odbcParams = base.GetScriptParameters(stageFilePath, fileGuid, fileDate);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid($"Start executing redshift load - {redshiftSqlScriptPath}")));

                    string sql = RedshiftRepository.PrepareCommandText(redshiftProcessSQL, odbcParams);
                    var result = RedshiftRepository.ExecuteRedshiftCommand(sql);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"Completed executing redshift load - {redshiftSqlScriptPath}; result: {result}")));

                    //Update and Delete Queue
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"Start update status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGUid: {queueItem.FileGUID}")));

                    base.UpdateQueueWithDelete(new[] { queueItem }, Constants.JobStatus.Complete, true);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"End update status to 'complete'. Deleting QueueItem:QueueID: {queueItem.ID}; FileGUid: {queueItem.FileGUID}")));

                    DeleteStageFiles(paths, queueItem.FileGUID);

                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid(
                            $"End processing - {CurrentIntegration.IntegrationName}; FileGUid: {queueItem.FileGUID}; path={pathDescription}")));
                }
                catch (Exception exc)
                {
                    exceptionCount++;
                    queueItem.StatusId = (int)Constants.JobStatus.Error;
                    queueItem.Status = Constants.JobStatus.Error.ToString();
                    JobService.Update(queueItem);
                    _logger.Log(Msg.Create(LogLevel.Error, _logger.Name,
                        PrefixJobGuid(
                            $"Processing error -> failed on: {queueItem.FileGUID} for EntityID: {queueItem.EntityID} FileDate: {queueItem.FileDate}  -> Exception: {exc.Message} - STACK {exc.StackTrace}")
                        , exc));
                }
            }//end foreach

            if (exceptionCount > 0)
            {
                throw new ErrorsFoundException($"Total errors: {exceptionCount}; Please check Splunk for more detail.");
            }
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

        ~DataLoadJob()
        {
            Dispose(false);
        }
    }
}

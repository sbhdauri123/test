using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.RUNDSP
{
    [Export("RUNDSP-MetadataDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        public void PreExecute()
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid($"{this.CurrentSource.SourceName} - IMPORT-PREEXECUTE {base.DefaultJobCacheKey}")));
            Stage = Constants.ProcessingStage.RAW;
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }

        public void Execute()
        {
            _logger.Log(
                Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE START {base.DefaultJobCacheKey}")));

            var queueItem = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID)
                .FirstOrDefault();
            string jobGUID = this.JED.JobGUID.ToString();

            var etl = new Greenhouse.DAL.ETLProvider();
            etl.SetJobLogGUID(jobGUID);
            var baseDestUri = base.GetDestinationFolder();
            string[] paths = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
            Uri path = RemoteUri.CombineUri(baseDestUri, paths);

            var fileList = queueItem.FileCollection.Where(f => f.SourceFileName.Contains("run1")).ToList();

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid(
                    $"Start loading {CurrentIntegration.IntegrationName} file->path:{path}; guid:{queueItem.FileGUID}")));

            etl.LoadRundspMetadataStageCollection(queueItem.FileGUID, path, base.SourceId, fileList,
                base.CurrentSource.PostProcessing);

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid($"End  loading {CurrentIntegration.IntegrationName}")));

            Data.Services.JobService.Delete<Queue>(queueItem.ID);
            _logger.Log(
                Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE END {base.DefaultJobCacheKey}")));
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

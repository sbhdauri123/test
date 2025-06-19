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

namespace Greenhouse.Jobs.TTD
{
    [Export("TTD-MetadataDataLoad", typeof(IDragoJob))]
    public class MetadataProcessing : Framework.BaseFrameworkJob, Infrastructure.IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        void IDragoJob.PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;

            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }

        void IDragoJob.Execute()
        {
            var queueItem = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).FirstOrDefault();

            var baseDestUri = base.GetDestinationFolder();
            string[] paths = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate) };
            Uri path = RemoteUri.CombineUri(baseDestUri, paths);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, String.Format("{0} - Starting ETL processing of metadata files.", base.JED.TriggerName)));
            DAL.ETLProvider.LoadTTDMetadata(queueItem.FileGUID, path, base.SourceId, base.IntegrationId, CurrentIntegration.CountryID, queueItem.FileCollection.ToList());
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, String.Format("{0} - Finished ETL processing of metadata files.", base.JED.TriggerName)));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, String.Format("{0} - Deleting Queue id: {1}.", base.JED.TriggerName, queueItem.ID)));
            Data.Services.JobService.Delete<Queue>(queueItem.ID);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, String.Format("{0} - Deleted Queue id: {1}.", base.JED.TriggerName, queueItem.ID)));
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        void IDragoJob.PostExecute()
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

        ~MetadataProcessing()
        {
            Dispose(false);
        }
    }
}

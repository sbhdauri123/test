using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.ComponentModel.Composition;

namespace Greenhouse.Jobs.SizmekMDX
{
    [Export("SizmekMDX-MetadataDataLoad", typeof(IDragoJob))]
    public class MetadataProcessing : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
        }

        public void Execute()
        {
            var queueItems =
                Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID);
            string jobGUID = this.JED.JobGUID.ToString();

            var etl = new Greenhouse.DAL.ETLProvider();
            etl.SetJobLogGUID(jobGUID);
            var baseDestUri = base.GetDestinationFolder();
            var ghostBatch = 18;

            foreach (Queue queueItem in queueItems)
            {
                var fileDateHour = (int)queueItem.FileDateHour;
                var fileDateHourPartition = GetHourPartition(fileDateHour);
                if (queueItem.FileDateHour == ghostBatch)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format(
                            "{0} - Archive for {1} contains empty files. Processing skipped for {2}, deleting record from the queue.",
                            jobGUID, queueItem.FileGUID, fileDateHourPartition)));
                    Data.Services.JobService.Delete<Queue>(queueItem.ID);
                    continue;
                }

                Uri path = RemoteUri.CombineUri(baseDestUri, GetDatedPartition(queueItem.FileDate));

                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    string.Format("{0} - Start processing {3} file->path:{1};guid:{2}; ", jobGUID, path,
                        queueItem.FileGUID, CurrentIntegration.IntegrationName)));

                etl.LoadMetadataSizmekMdx(queueItem.FileGUID, path, base.SourceId, base.IntegrationId,
                    CurrentIntegration.CountryID, queueItem.FileName,
                    base.CurrentSource.PostProcessing);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    string.Format("{0} - End  processing {1}", jobGUID, CurrentIntegration.IntegrationName)));
                Data.Services.JobService.Delete<Queue>(queueItem.ID);
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

        ~MetadataProcessing()
        {
            Dispose(false);
        }
    }
}
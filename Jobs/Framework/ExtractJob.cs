using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using ICSharpCode.SharpZipLib.Zip;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;

namespace Greenhouse.Jobs.Framework
{
    [Export("GenericExtractJob", typeof(IDragoJob))]
    public class ExtractJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC;
        private Uri _baseDestUri;
        private SourceJobStep _nextStep;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            _baseDestUri = GetDestinationFolder();
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(base.IntegrationId);
            _nextStep = JED.ExecutionPath.GotoNextStep();
        }

        public void Execute()
        {
            RAC = GetS3RemoteAccessClient();
            IEnumerable<IFileItem> queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{0} items retreived from queue for Integration: {1} - {2}", queueItems.Count(), CurrentIntegration.IntegrationID, CurrentIntegration.IntegrationName)));
            foreach (IFileItem fi in queueItems)
            {
                string[] paths = new string[] { fi.EntityID.ToLower(), GetDatedPartition(fi.FileDate) };
                Uri sourceBucket = RemoteUri.CombineUri(this._baseDestUri, paths);
                Uri destBucket = new Uri(sourceBucket.ToString().Replace(Constants.ProcessingStage.RAW.ToString().ToLower(), Constants.ProcessingStage.STAGE.ToString().ToLower()));
                IFile sourceZip = RAC.WithFile(RemoteUri.CombineUri(sourceBucket, fi.FileName));
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Ready to extract: {0} to {1}", sourceZip.FullName, destBucket.ToString())));
                fi.Status = Constants.JobStatus.Running.ToString();
                fi.Step = Constants.JobStep.Extract.ToString();
                Data.Services.JobService.Update((Queue)fi);
                List<FileCollectionItem> extractedFiles = new List<FileCollectionItem>();
                using (ZipInputStream inStream = new ZipInputStream(sourceZip.Get()))
                {
                    ZipEntry zipEntry = inStream.GetNextEntry();
                    while (zipEntry != null)
                    {
                        IFile destFile = RAC.WithFile(RemoteUri.CombineUri(destBucket, zipEntry.Name));
                        using (var entryStream = new MemoryStream())
                        {
                            inStream.CopyTo(entryStream);
                            entryStream.Position = 0;
                            destFile.Put(entryStream);
                        }
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Extracted: {0} ", destFile.FullName)));
                        extractedFiles.Add(new FileCollectionItem()
                        {
                            FilePath = zipEntry.Name,
                            FileSize = zipEntry.Size,
                            SourceFileName = null
                        });
                        zipEntry = inStream.GetNextEntry();
                    }
                }//using (ZipInputStream inStream = new ZipInputStream(sourceZip.Get())) {
                fi.FileCollectionJSON = JsonConvert.SerializeObject(extractedFiles);
                fi.Status = Constants.JobStatus.Complete.ToString();
                Data.Services.JobService.Update((Queue)fi);
                Greenhouse.Data.Model.Core.JobExecutionDetails newJED = base.CloneJED();
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Next Step is: {0} JED.ExecutionPath.CurrentStep: {1}", _nextStep, newJED.ExecutionPath.CurrentStep)));
                if (_nextStep != null)
                {
                    newJED.ResetExecutionGuid();
                    newJED.Step = newJED.ExecutionPath.CurrentStep.Step.ParseEnum<Constants.JobStep>();
                    newJED.JobProperties[Constants.US_SOURCE_ID] = CurrentIntegration.SourceID;
                    newJED.JobProperties[Constants.US_INTEGRATION_ID] = CurrentIntegration.IntegrationID;
                    newJED.JobProperties[Constants.CP_FILE_GUID] = fi.FileGUID.ToString();
                    string replace = string.Format("{0}/", Amazon.RegionEndpoint.GetBySystemName(Greenhouse.Configuration.Settings.Current.AWS.Region).GetEndpointForService("s3").Hostname);
                    newJED.JobProperties[Constants.CP_FILE_PATH] = destBucket.ToString().Replace(replace, string.Empty);

                    base.ScheduleDynamicJob(newJED);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Job {0} batched and scheduled for integration: {1}", newJED.ExecutionPath.CurrentStep.SourceJobStepName, CurrentIntegration.IntegrationID)));
                }
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
                RAC?.Dispose();
            }
        }
        ~ExtractJob()
        {
            Dispose(false);
        }
    }
}

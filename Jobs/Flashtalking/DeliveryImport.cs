using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Flashtalking
{
    [Export("Flashtalking-DeliveryImportJob", typeof(IDragoJob))]
    public class DeliveryImport : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient remoteAccessClient;
        private List<IFile> remoteAccessClientFilesMatchingDone;
        private List<IFile> remoteAccessClientFilesPending;
        private Uri baseDestUri;
        private string jobGuid => base.JED.JobGUID.ToString();

        void IDragoJob.PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
            string.Format("{2} - {1}-IMPORT-PREEXECUTE {0}", this.GetJobCacheKey(), this.CurrentSource, jobGuid)));
        }

        void IDragoJob.Execute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE START {0}", this.GetJobCacheKey(), jobGuid)));
            Queue? importQueueFile = null;

            //Get all processed files.
            var jobLogProcessedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            remoteAccessClient = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{3} - Integration: {0}, fetching source files against regex: {1}. File Start Date is: {2}", CurrentIntegration.IntegrationName, regCod.FileNameRegex, CurrentIntegration.FileStartDate, jobGuid)));

            //Filter files by integration's regex mask.
            var remoteAccessClientFilesToBeProcessed = remoteAccessClient.WithDirectory().GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) &&
                     regCod.FileNameDate >= CurrentIntegration.FileStartDate).ToList();

            //Only include match dot done files            
            var remoteAccessClientDoneFilesToBeProcessed = remoteAccessClientFilesToBeProcessed.Where(f => f.Extension == Constants.DONE_EXT).Select(f => f.Name.Substring(0, f.Name.Length - Constants.DONE_EXT.Length)).ToList<string>();

            //Done file not needed hence removing.
            remoteAccessClientFilesToBeProcessed.RemoveAll(f => f.Extension == Constants.DONE_EXT);

            //Only process file when the file has a correspoding done file otherwise log and ignore.
            remoteAccessClientFilesMatchingDone = remoteAccessClientFilesToBeProcessed.Where(f => remoteAccessClientDoneFilesToBeProcessed.Any(d => d.Equals(f.Name, StringComparison.CurrentCultureIgnoreCase))).ToList<IFile>();

            //Get remote access client files pending to be imported that has not been processed.
            remoteAccessClientFilesPending = remoteAccessClientFilesMatchingDone.Where(x => !jobLogProcessedFiles.Any(y => y.FileName.Equals(x.Name))).OrderBy(p => p.LastWriteTimeUtc).ToList();
            if (remoteAccessClientFilesPending?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - All files have already been processed for integration: {0} and integration date: {1}", CurrentIntegration.IntegrationName, CurrentIntegration.FileStartDate, jobGuid)));
                return;
            }
            var totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)remoteAccessClientFilesPending.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format(
                    "{7} - Integration: {0}, Source Files ({1}): {2}, Destination Files ({3}): {4} Preparing: {5} files for import. {6} total bytes.",
                    CurrentIntegration.IntegrationName,
                    remoteAccessClient.GetType(),
                    remoteAccessClientFilesMatchingDone.Count,
                    "S3",
                    jobLogProcessedFiles.Count(),
                    remoteAccessClientFilesPending.Count,
                    totalBytes,
                    jobGuid)));

            foreach (IFile incomingFile in remoteAccessClientFilesPending)
            {
                try
                {
                    importQueueFile = new Queue()
                    {
                        FileGUID = Guid.NewGuid(),
                        FileName = incomingFile.Name,
                        FileSize = incomingFile.Length,
                        IntegrationID = CurrentIntegration.IntegrationID,
                        SourceID = CurrentSource.SourceID,
                        Status = Constants.JobStatus.Complete.ToString(),
                        StatusId = (int)Constants.JobStatus.Complete,
                        JobLogID = this.JobLogger.JobLog.JobLogID,
                        Step = JED.Step.ToString(),
                        DeliveryFileDate = incomingFile.LastWriteTimeUtc
                    };

                    SourceFile matchingSourceFile = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (matchingSourceFile == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Filename: {0} skipped because no matching source file found", incomingFile.Name, jobGuid)));
                        continue;
                    }

                    importQueueFile.SourceFileName = matchingSourceFile.SourceFileName;
                    if (matchingSourceFile.FileRegexCodec.TryParse(incomingFile.Name))
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{2} - incomingFile.Name: {1}. sf: {0}", JsonConvert.SerializeObject(matchingSourceFile), incomingFile.Name, jobGuid)));
                        importQueueFile.FileDate = matchingSourceFile.FileRegexCodec.FileNameDate.Value;
                        importQueueFile.FileDateHour = matchingSourceFile.FileRegexCodec.FileNameHour;
                        importQueueFile.EntityID = matchingSourceFile.FileRegexCodec.EntityId;
                    }
                    else
                    {
                        importQueueFile.FileDate = incomingFile.LastWriteTimeUtc;
                    }

                    //raw/flashtalking-delivery/date 
                    string[] paths = new string[] { importQueueFile.EntityID.ToLower(), GetDatedPartition(importQueueFile.FileDate), importQueueFile.FileName };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, String.Format("{2} - destUri: {0}. paths: {1} ", JsonConvert.SerializeObject(destUri), JsonConvert.SerializeObject(paths), jobGuid)));

                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    base.UploadToS3(incomingFile, (S3File)destFile, paths);

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    importQueueFile.Status = Common.Constants.JobStatus.Complete.ToString();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Adding to queue: {0}", JsonConvert.SerializeObject(importQueueFile), jobGuid)));

                    Data.Services.JobService.Add(importQueueFile);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE END {0}", this.GetJobCacheKey(), jobGuid)));
                }
                catch (Exception exc)
                {
                    importQueueFile.Status = Common.Constants.JobStatus.Error.ToString();
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{3} - Import failed on file {0} - Size: {1}, Exception was: {2}", incomingFile.Uri, incomingFile.Length, exc.Message, jobGuid)));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE ERROR {0}", this.GetJobCacheKey(), jobGuid)));
                    throw;
                }
            }
        }
        void IDragoJob.PostExecute() { }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                remoteAccessClient?.Dispose();
            }
        }

        ~DeliveryImport()
        {
            Dispose(false);
        }
        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }
    }
}
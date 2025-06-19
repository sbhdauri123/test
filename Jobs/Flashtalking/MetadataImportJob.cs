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
using System.Text.RegularExpressions;

namespace Greenhouse.Jobs.Flashtalking
{
    [Export("Flashtalking-MetadataImportJob", typeof(IDragoJob))]
    public partial class MetadataImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient remoteAccessClient;
        private Queue importQueueBatch;
        private Uri baseDestUri;
        private string jobGuid { get { return this.JED.JobGUID.ToString(); } }
        private List<Queue> importQueueFileCollection { get; set; }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
        }

        public void Execute()
        {
            //Get all processed files.
            var jobLogProcessedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            //Initialize the appropriate client for this integration.
            remoteAccessClient = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{3} - Integration: {0}, will match source files against regex: {1}. Integration date is: {2}", CurrentIntegration.IntegrationName, regCod.FileNameRegex, CurrentIntegration.FileStartDate, jobGuid)));

            //Filter files by integration's regex mask.
            var remoteAccessClientFilesToBeProcessed = remoteAccessClient.WithDirectory().GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) &&
                    regCod.FileNameDate >= CurrentIntegration.FileStartDate).ToList();

            //Filter file batches by done match table file and group by date.
            Regex regexForRemoteAccessClientFileBatchesToBeProcessed = RACFileBatchesToBeProcessedRegex();
            var remoteAccessClientFileBatchesToBeProcessed = remoteAccessClientFilesToBeProcessed.
                Where(f => f.Name.Contains(Constants.DONE_MATCH_TABLES)).
                Select(f2 => regexForRemoteAccessClientFileBatchesToBeProcessed.Match(f2.Name).Value).ToList<string>();
            if (remoteAccessClientFileBatchesToBeProcessed?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - Integration: {0}, .done table file not found for the Integration date specified: {1}.", CurrentIntegration.IntegrationName, CurrentIntegration.FileStartDate, jobGuid)));
                return;
            }

            //Done table file not needed hence removing.
            remoteAccessClientFilesToBeProcessed.RemoveAll(f => f.Extension.Equals(Constants.DONE_EXT, StringComparison.CurrentCultureIgnoreCase));

            //Get any pending files batches to be imported.
            var remoteAccessClientFileBatchesPending = new List<string>();
            var jobLogFileBatchesProcessed = jobLogProcessedFiles.Select(f => f.FileDate.ToString("yyyy-MM-dd")).ToList<string>();
            remoteAccessClientFileBatchesPending = (jobLogFileBatchesProcessed?.Count < 1) ?
                remoteAccessClientFileBatchesToBeProcessed :
                remoteAccessClientFileBatchesToBeProcessed.Except(jobLogFileBatchesProcessed).Select(f => f).ToList();

            if (remoteAccessClientFileBatchesPending?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - All files have already been processed for integration: {0} and integration date: {1}", CurrentIntegration.IntegrationName, CurrentIntegration.FileStartDate, jobGuid)));
                return;
            }

            //Remove files that are already processed (not pending).
            remoteAccessClientFilesToBeProcessed.RemoveAll(file => !remoteAccessClientFileBatchesPending.Any(date => file.Name.Contains(date)));

            //Create file batch dictionary 
            var remoteAccessClientFilesToBeProcessedDictionary =
            remoteAccessClientFilesToBeProcessed.
            GroupBy(fileItems => regexForRemoteAccessClientFileBatchesToBeProcessed.Match(fileItems.Name).Value).
            ToDictionary(dictionaryFileItem => dictionaryFileItem.Key, fileItem => fileItem.ToList());

            int hour = 0;
            //Process each pending file batch.
            foreach (var incomingFileBatch in remoteAccessClientFilesToBeProcessedDictionary)
            {
                importQueueFileCollection = new List<Queue>();
                importQueueBatch = new Queue()
                {
                    FileGUID = Guid.NewGuid(),
                    FileName = "Metadata",
                    IntegrationID = CurrentIntegration.IntegrationID,
                    SourceID = CurrentSource.SourceID,
                    Status = Constants.JobStatus.Pending.ToString(),
                    StatusId = (int)Constants.JobStatus.Running,
                    JobLogID = this.JobLogger.JobLog.JobLogID,
                    Step = JED.Step.ToString(),
                    SourceFileName = "Metadata",
                    FileDateHour = hour,
                    FileDate = DateTime.Parse(incomingFileBatch.Key).ToUniversalTime()
                };

                string totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)incomingFileBatch.Value.Sum(s => s.Length));
                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    string.Format("{6} - Integration: {0}, Source Files ({1}): {2}, Destination Files ({3}): {4} files for import. {5} total bytes.",
                        base.CurrentIntegration.IntegrationName,
                        remoteAccessClient.GetType().ToString(),
                        incomingFileBatch.Value.Count,
                        "S3",
                        incomingFileBatch.Value.Count,
                        totalBytes,
                        jobGuid)));

                /**
                 ** We need to account for duplicate file types, check that we have all distinct file types.
                 ** Make sure we have a matching file type in [SourceFiles] for each individual file to be processed.
                 **/
                var fileTypes = SourceFiles.Where(s => incomingFileBatch.Value.Any(pf => s.FileRegexCodec.FileNameRegex.IsMatch(pf.Name))).Select(x => x.SourceFileName);
                if (SourceFiles.Count() != fileTypes.Distinct().Count())
                {
                    string errMsg = string.Format("{1} - Error {3} metadata. Current import file count {0}. Requires: {2} source files. Not all files types are ready to be imported for the jobs to run", fileTypes.Distinct().Count(), jobGuid, SourceFiles.Count(), Constants.DSN_FLASHTALKING);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, errMsg));
                    base.JobLogger.JobLog.Message = errMsg;
                    return;
                }

                //Download pending files in batch.
                foreach (IFile incomingFile in incomingFileBatch.Value)
                {
                    try
                    {
                        var matchingSourceFile = (SourceFile)SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                        if (matchingSourceFile == null)
                        {
                            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Filename: {0} skipped because no matching source file found", incomingFile.Name, jobGuid)));
                            continue;
                        }

                        //raw/flashtalking-metadata/date
                        string[] paths = new string[] { GetDatedPartition(importQueueBatch.FileDate), incomingFile.Name };
                        Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                        IFile destFile = new S3File(destUri, GreenhouseS3Creds);
                        incomingFile.CopyTo(destFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{1} - File imported to: {0}", destUri, jobGuid)));

                        filesIn++;
                        bytesIn += incomingFile.Length;
                        //Add the completed transfer file collection to the transfer log.
                        importQueueFileCollection.Add(new Queue { SourceFileName = matchingSourceFile.SourceFileName, FileName = incomingFile.Name, FileSize = incomingFile.Length });
                    }
                    catch (Exception exc)
                    {
                        //Make sure we log the failure to the transfer logs.                     
                        logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{3} - Import failed on file {0} - Size: {1}, Exception was: {2}", incomingFile.Uri, incomingFile.Length, exc.Message, jobGuid)));
                        throw;
                    }
                }

                QueueImportFiles();
            }
        }

        public void QueueImportFiles()
        {
            try
            {
                //store files to be processed csv of FileType:FilePath.
                var files = this.importQueueFileCollection.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName,
                    FileSize = x.FileSize
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{4} - Integration: {0}, matched {1} files from source using regex: {2}. Integration date is: {3}", CurrentIntegration.IntegrationName, this.importQueueFileCollection.Count, filesJSON, CurrentIntegration.FileStartDate, jobGuid)));

                importQueueBatch.FileName = "metadata.json";
                importQueueBatch.Status = Common.Constants.JobStatus.Complete.ToString();
                importQueueBatch.StatusId = (int)Constants.JobStatus.Complete;
                importQueueBatch.FileCollectionJSON = filesJSON;
                importQueueBatch.FileSize = bytesIn;
                Data.Services.JobService.Add(importQueueBatch);

                string msg = string.Format("{1} - Successfully queued {2} metadata files {0}.", filesJSON, jobGuid, Constants.DSN_FLASHTALKING);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, msg));
            }
            catch (Exception exc)
            {
                string errMsg = string.Format("{1} - Error queuing {2} metadata files {0}", JsonConvert.SerializeObject(this.importQueueFileCollection), jobGuid, Constants.DSN_FLASHTALKING);
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, exc));
                throw;
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
                remoteAccessClient?.Dispose();
            }
        }

        ~MetadataImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        [GeneratedRegex(@"\d{4}-\d{2}-\d{2}")]
        private static partial Regex RACFileBatchesToBeProcessedRegex();
    }
}
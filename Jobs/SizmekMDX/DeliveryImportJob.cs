using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
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
using System.Linq;
using System.Text.RegularExpressions;

namespace Greenhouse.Jobs.SizmekMDX
{
    [Export("SizmekMDX-DeliveryImportJob", typeof(IDragoJob))]
    public partial class DeliveryImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
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
            //Get all imported files.
            var jobLogimportedFiles = JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            //Get RegexMask from filetype + integration to filter files from the remote access client
            var RegexMask = String.Join("|", SourceFiles.Select(x => x.RegexMask)) + "|" + CurrentIntegration.RegexMask;

            //Initialize the appropriate client for this integration.
            remoteAccessClient = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Match source files against regex: {regCod.FileNameRegex}.")));

            //Get S3 files filter by integration's regex mask and integration date.
            var remoteAccessClientFiles = remoteAccessClient.WithDirectory().GetFiles().Where(f => regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) && regCod.FileNameDate >= CurrentIntegration.FileStartDate).ToList();

            //Remove imported batches
            remoteAccessClientFiles.RemoveAll(remoteFile => jobLogimportedFiles.Any(imported => IsMatchFilename(imported.FileName, remoteFile.Name)));
            //Get .done file collection
            var dotDoneFileCollection = remoteAccessClientFiles.Where(file => file.Extension == Constants.DONE_EXT).Select(file => file.Name).ToList();
            if (dotDoneFileCollection?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"All files have already been imported.")));
                return;
            }

            /*
             * DIAT-1338 - Acceptances Criteria: When .done file per batch exists (hour file was drop) then 
             * all corresponding file types should be included 
             * (eg. Batch: 00 will include fileType conversion00, sitevisit00, impression00, richevent00).
             */

            // Get the expected batch size
            var expectedBatchSize = int.Parse(SetupService.GetById<Lookup>(Constants.BATCH_SIZE_SIZMEK_LOG).Value);

            //Group files by .done file Batches
            var dotDoneGroup = remoteAccessClientFiles.Where(file => file.Extension != Constants.DONE_EXT).GroupBy(file => ConvertToDotDoneName(file.Name)).ToDictionary(dotDone => dotDone.Key.ToLower(), files => files.ToList());

            //Check if .done file collection matches group .done done keys, if so continue import otherwise do not import.
            var dotDoneBatchesPending = dotDoneGroup.Where(group => dotDoneFileCollection.Any(dotDoneFile => IsMatchFilename(group.Key, dotDoneFile)) && group.Value.Count == expectedBatchSize);
            if (dotDoneBatchesPending?.Count() < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"{Constants.DONE_EXT}  table file not found for the Integration.")));
                return;
            }

            //Check if any batches count mismatch with the [expectedBatchSize].
            foreach (var batch in dotDoneGroup.Where(group => group.Value.Count != expectedBatchSize))
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Batch for {batch.Key} mismatch, {batch.Value.Count} files found and {expectedBatchSize} files expected. Integration: {CurrentIntegration.IntegrationName} and integration date: {CurrentIntegration.FileStartDate}.")));
            }

            //import each pending file batch.
            ImportPendingBatches(dotDoneBatchesPending);
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

        ~DeliveryImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        private void ImportPendingBatches(IEnumerable<KeyValuePair<string, List<IFile>>> dotDoneBatchesPending)
        {
            foreach (var incomingDotDoneBatch in dotDoneBatchesPending)
            {
                var date = DateRegex().Match(incomingDotDoneBatch.Key).Value;
                var formattedDate = DateTime.ParseExact(date, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture).ToUniversalTime();

                importQueueFileCollection = new List<Queue>();
                importQueueBatch = new Queue()
                {
                    FileGUID = Guid.NewGuid(),
                    FileName = incomingDotDoneBatch.Key,
                    IntegrationID = CurrentIntegration.IntegrationID,
                    SourceID = CurrentSource.SourceID,
                    Status = Constants.JobStatus.Pending.ToString(),
                    StatusId = (int)Constants.JobStatus.Running,
                    JobLogID = this.JobLogger.JobLog.JobLogID,
                    Step = JED.Step.ToString(),
                    SourceFileName = "Log",
                    FileDateHour = Convert.ToInt32(incomingDotDoneBatch.Key.Substring(incomingDotDoneBatch.Key.Length - (Constants.DONE_EXT.Length + 2), 2)),
                    FileDate = formattedDate
                };

                var totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)incomingDotDoneBatch.Value.Sum(s => s.Length));
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Source Files ({remoteAccessClient.GetType().ToString()}): {incomingDotDoneBatch.Value.Count} - Destination Files (S3): {incomingDotDoneBatch.Value.Count} files for import. {totalBytes} total bytes.")));

                //Download pending files in batch.
                ImportPendingFiles(incomingDotDoneBatch);

                QueueImportFiles(incomingDotDoneBatch.Key);
            }
        }

        private void ImportPendingFiles(KeyValuePair<string, List<IFile>> incomingDotDoneBatch)
        {
            foreach (IFile incomingFile in incomingDotDoneBatch.Value)
            {
                try
                {
                    var matchingSourceFile = (SourceFile)SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (matchingSourceFile == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Filename: {incomingFile.Name} skipped because no matching source file found.")));
                        continue;
                    }

                    importQueueBatch.SourceFileName = matchingSourceFile.SourceFileName;

                    //s3://datalake-americas/raw/sizmekmdx-metadata/date
                    string[] paths = new string[] { GetDatedPartition(importQueueBatch.FileDate), incomingFile.Name };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);
                    incomingFile.CopyTo(destFile, true);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"File imported to: {destUri}.")));

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    //Add the completed transfer file collection to the transfer log.
                    importQueueFileCollection.Add(new Queue { SourceFileName = matchingSourceFile.SourceFileName, FileName = incomingFile.Name, FileSize = incomingFile.Length });
                }
                catch (Exception exc)
                {
                    //Make sure we log the failure to the transfer logs.                     
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, BuildLogString($"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length}, Exception was: {exc.Message}.")));
                    throw;
                }
            }
        }

        /// <summary>
        /// Imports files to [Queue]
        /// </summary>
        /// <param name="incomingFileBatch"></param>
        public void QueueImportFiles(string incomingFileBatch)
        {
            try
            {
                //store files to be imported csv of FileType:FilePath.
                var files = this.importQueueFileCollection.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName,
                    FileSize = x.FileSize
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                   BuildLogString($"matched {this.importQueueFileCollection.Count} files from source using regex: {filesJSON}.")));

                importQueueBatch.FileName = incomingFileBatch;
                importQueueBatch.Status = Common.Constants.JobStatus.Complete.ToString();
                importQueueBatch.StatusId = (int)Constants.JobStatus.Complete;
                importQueueBatch.FileCollectionJSON = filesJSON;
                importQueueBatch.FileSize = bytesIn;
                JobService.Add(importQueueBatch);

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    BuildLogString($"Successfully queued {CurrentSource.SourceName} log files {filesJSON}.")));
            }
            catch (Exception exc)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                    BuildLogString($"Error queuing {CurrentSource.SourceName} log files {JsonConvert.SerializeObject(this.importQueueFileCollection)}.")
                    , exc));
                throw;
            }
        }

        /// <summary>
        /// Anticipates a known misspell [vist] instead of [visit]
        /// to make sure that if it fix in the future the application will not break.
        /// </summary>
        /// <param name="filename1"></param>
        /// <param name="filename2"></param>
        /// <returns></returns>
        private static bool IsMatchFilename(string filename1, string filename2)
        {
            var name = filename2.Contains("sitevist") ? filename2.Replace("sitevist", "sitevisit") : filename2;//Amy K. said to go ahead with misspelling on S3 files.
            return name.Equals(filename1, StringComparison.CurrentCultureIgnoreCase);
        }

        /// <summary>
        /// Group by .done filename by renaming the Key to the correspoding .done batch.
        /// These Keys will be use to compare if the file .done exists in S3
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public static string ConvertToDotDoneName(string fileName)
        {
            var nameArray = fileName.Split('_');
            var nameArrayLength = nameArray.Length;
            var colNames = nameArray.Where((name, index) => index > 0 && index != (nameArrayLength - 2));
            return string.Join("_", colNames).Replace(".tar.gz", ".done");
        }

        /// <summary>
        /// Appends information about JobGuid, integrationName and intgrationStarDate to the beginning of the [message]
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public string BuildLogString(string message)
        {
            return
                $"{jobGuid} - " +
                $"Integration name: {CurrentIntegration.IntegrationName} - " +
                $"Integration date: {CurrentIntegration.FileStartDate} - " +
                message;
        }

        [GeneratedRegex(@"\d{4}\d{2}\d{2}")]
        private static partial Regex DateRegex();
    }
}
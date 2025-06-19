using Amazon.S3.Transfer;
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

namespace Greenhouse.Jobs.SizmekMDX
{
    [Export("SizmekMDX-MetadataImportJob", typeof(IDragoJob))]
    public class MetadataImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient remoteAccessClient;
        private Uri baseDestUri;
        private string jobGuid { get { return this.JED.JobGUID.ToString(); } }

        private List<IFile> filesPendingWithMatchingDotDoneFile;
        private List<IFile> filesPendingNotMatchingDotDoneFile;
        private List<IFile> filesPending;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
        }

        public void Execute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"EXECUTE START {this.GetJobCacheKey()}.")));

            Queue? importQueueFile = null;

            //Get all imported files.
            var jobLogImportedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            //Get RegexMask from filetype + integration to filter files from the remote access client
            var RegexMask = String.Join("|", SourceFiles.Select(x => x.RegexMask)) + "|" + CurrentIntegration.RegexMask;

            remoteAccessClient = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Fetching source files against regex: {regCod.FileNameRegex}.")));

            //Filter files by integration's regex mask.
            var remoteAccessClientFiles = remoteAccessClient.WithDirectory().GetFiles().
                Where(f => regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) && regCod.FileNameDate >= CurrentIntegration.FileStartDate).ToList();

            //Get .done files
            var dotDoneFileCollection = remoteAccessClientFiles.
                Where(dotDoneFile => dotDoneFile.Extension == Constants.DONE_EXT).
                Select(dotDoneFile => dotDoneFile.Name.Replace(Constants.DONE_EXT, "")).ToList<string>();

            //Remove .done files
            remoteAccessClientFiles.RemoveAll(f => f.Extension.Equals(Constants.DONE_EXT, StringComparison.CurrentCultureIgnoreCase));

            //Only import files when the files have correspoding .done files
            filesPendingWithMatchingDotDoneFile = remoteAccessClientFiles.Where(importfiles => dotDoneFileCollection.Any(dotDoneName => importfiles.Name.StartsWith(dotDoneName, StringComparison.CurrentCultureIgnoreCase))).ToList<IFile>();

            //Logged files when the files do not have correspoding .done files
            filesPendingNotMatchingDotDoneFile = remoteAccessClientFiles.Where(importfiles => !dotDoneFileCollection.Any(dotDoneName => importfiles.Name.StartsWith(dotDoneName, StringComparison.CurrentCultureIgnoreCase))).ToList<IFile>();
            foreach (var notMatchingDotDoneFile in filesPendingNotMatchingDotDoneFile)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"File: {notMatchingDotDoneFile.Name} does not match any .done file.")));
            }

            //Get remote access client files pending to be imported that has not been imported.
            filesPending = filesPendingWithMatchingDotDoneFile.
                Where(pendingFile => !jobLogImportedFiles.Any(importedFile => importedFile.FileName.Equals(pendingFile.Name, StringComparison.CurrentCultureIgnoreCase))).
                OrderBy(pendingFile => pendingFile.LastWriteTimeUtc).ToList();
            if (filesPending?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"All files have already been imported.")));
                return;
            }
            var totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)filesPending.Sum(s => s.Length));
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Source Files ({remoteAccessClient.GetType()}): {filesPendingWithMatchingDotDoneFile.Count}, Destination Files ({filesPendingWithMatchingDotDoneFile.Count}): S3 Preparing: {filesPending.Count} files for import. {totalBytes} total bytes.")));

            foreach (IFile incomingImportFile in filesPending)
            {
                try
                {
                    importQueueFile = new Queue()
                    {
                        FileGUID = Guid.NewGuid(),
                        FileName = incomingImportFile.Name,
                        FileSize = incomingImportFile.Length,
                        IntegrationID = CurrentIntegration.IntegrationID,
                        SourceID = CurrentSource.SourceID,
                        Status = Constants.JobStatus.Complete.ToString(),
                        StatusId = (int)Constants.JobStatus.Complete,
                        JobLogID = this.JobLogger.JobLog.JobLogID,
                        Step = JED.Step.ToString(),
                        DeliveryFileDate = incomingImportFile.LastWriteTimeUtc,
                        FileDateHour = Convert.ToInt32(incomingImportFile.Name.Substring(incomingImportFile.Name.Length - 9, 2)),
                    };

                    SourceFile matchingSourceFile = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingImportFile.Name));
                    if (matchingSourceFile == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Filename: {incomingImportFile.Name} skipped because no matching source file found.")));
                        continue;
                    }

                    importQueueFile.SourceFileName = matchingSourceFile.SourceFileName;
                    if (matchingSourceFile.FileRegexCodec.TryParse(incomingImportFile.Name))
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"IncomingFile.Name: {incomingImportFile.Name}. sf: {JsonConvert.SerializeObject(matchingSourceFile)}.")));
                        importQueueFile.FileDate = matchingSourceFile.FileRegexCodec.FileNameDate.Value;
                    }

                    //s3://datalake-americas/raw/sizmekmdx-metadata/date
                    string[] paths = new string[] { GetDatedPartition(importQueueFile.FileDate), importQueueFile.FileName };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"DestUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}.")));

                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);
                    if (incomingImportFile.Length > S3File.MAX_PUT_SIZE)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"FileLength for file {incomingImportFile.Name} is {incomingImportFile.Length}.")));

                        Uri tempDestUri = RemoteUri.CombineUri(new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                        FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                        if (!tempDestFile.Directory.Exists) { tempDestFile.Directory.Create(); }
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"Importing file to file system first: {tempDestUri}.")));

                        incomingImportFile.CopyTo(tempDestFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"Moving to S3 : {destUri}.")));

                        Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                        TransferUtility transferUtility = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                        transferUtility.UploadAsync(tempDestFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"TransferUtility S3 URI {destUri} upload complete.")));
                        tempDestFile.Delete();
                    }
                    else
                    {
                        incomingImportFile.CopyTo(destFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, BuildLogString($"File imported to: {destUri}.")));
                    }

                    filesIn++;
                    bytesIn += incomingImportFile.Length;
                    importQueueFile.Status = Common.Constants.JobStatus.Complete.ToString();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"Adding to queue: {JsonConvert.SerializeObject(importQueueFile)}.")));

                    Data.Services.JobService.Add(importQueueFile);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"EXECUTE END {this.GetJobCacheKey()}.")));
                }
                catch (Exception exc)
                {
                    importQueueFile.Status = Common.Constants.JobStatus.Error.ToString();
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, BuildLogString($"Import failed on file {incomingImportFile.Uri} - Size: {incomingImportFile.Length}, Exception was: {exc.Message}.")));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, BuildLogString($"EXECUTE ERROR {this.GetJobCacheKey()}.")));
                    throw;
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
    }
}
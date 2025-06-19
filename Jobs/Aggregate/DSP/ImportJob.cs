using Amazon.S3.Transfer;
using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Framework;
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

namespace Greenhouse.Jobs.Aggregate.DSP
{
    [Export("GenericAggregateDSPImportJob", typeof(IDragoJob))]
    public class ImportJob : BaseFrameworkJob, IDragoJob
    {
        private static Logger logger { get; set; } = LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC { get; set; }
        private Queue importFile { get; set; }
        private SourceFile CurrentSourceFile { get; set; }
        private List<Queue> importQueueFileCollection { get; set; }

        public void PreExecute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"IMPORT-PREEXECUTE {GetJobCacheKey()}"));
            Initialize();
        }

        public void Execute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"EXECUTE START {GetJobCacheKey()}")));

            //Initialize the appropriate client for this integration.
            RAC = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"will match source files against regex: {regCod.FileNameRegex}. Integration date is: {CurrentIntegration.FileStartDate}")));

            //Get remote files using integration's regex mask.
            var remoteFiles = RAC.WithDirectory().GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) &&
                    regCod.FileNameDate >= CurrentIntegration.FileStartDate).ToList();

            //Get FileDate for imported files (by date)
            var importedFilesByDate = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID).Select(f => f.FileDate);

            //Generated FileDate for remote files based on the date in the file name
            var remoteFilesByDate = remoteFiles.Where(remote => regCod.TryParse(remote.Name))
                .Select(remoteByDate => new { remoteByDate, FileDate = regCod.FileNameDate.Value }).ToList();

            //Only import remote file missing (by date)
            var remoteFilesMissing = remoteFilesByDate.Where(remote => !importedFilesByDate.Any(imported => imported == remote.FileDate));

            if (remoteFilesMissing?.Count() < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"All files have already been imported.")));
                return;
            }

            //Create file group by date and import
            var remoteFileGroupsMissing = remoteFilesMissing.GroupBy(remote =>
                remote.FileDate,
                remote => remote.remoteByDate,
                (key, group) => new { FileDate = key, importingFileCollection = group.ToList() });

            string totalBytes = UtilsText.GetFormattedSize(remoteFileGroupsMissing.Sum(s => s.importingFileCollection.Count));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixIntegration($"Source Files ({RAC.GetType().ToString()}): {remoteFilesMissing.Count()}, " +
                $"Destination Files (S3): {importedFilesByDate.Count()} Preparing: {remoteFilesMissing.Count()} files for import. {totalBytes} total bytes.")));

            //importing missing files
            foreach (var importingGroup in remoteFileGroupsMissing)
            {
                var importingFileCollection = importingGroup.importingFileCollection;

                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    PrefixIntegration(
                        $"Source Files ({RAC.GetType().ToString()}): {remoteFilesMissing.Count()}, " +
                        $"Destination Files (S3): {remoteFilesMissing.Count()} files for import.")));

                /**
                 ** We need to account for duplicate file types. Check that we have all distinct file types.
                 ** Make sure we have a matching file type in [SourceFiles] for each individual file to be imported.
                 **/
                var importingSourceFiles = SourceFiles.Where(s => importingFileCollection.Any(pf => s.FileRegexCodec.FileNameRegex.IsMatch(pf.Name))).Select(x => x.SourceFileName);
                if (SourceFiles.Count() != importingSourceFiles.Distinct().Count())
                {
                    string errMsg = PrefixIntegration(
                        $"All files are not ready for {CurrentSource.SourceName}. For date {importingGroup.FileDate}, the current import file count {importingSourceFiles.Distinct().Count()} ({string.Join(",", importingSourceFiles.Distinct())}). " +
                        $"Requires: {SourceFiles.Count()} source files. Not all files types are ready to be imported for the jobs to run");
                    logger.Log(Msg.Create(LogLevel.Warn, logger.Name, errMsg));
                    JobLogger.JobLog.Message = $"Warning. Some group of files were not ready to be imported - some files are missing. Search for 'All files not ready for {CurrentSource.SourceName}' in splunk for more details";
                    continue;
                }

                logger.Log(Msg.Create(LogLevel.Warn, logger.Name, $"All files ready for date {importingGroup.FileDate}"));
                CopyFileCollection(importingFileCollection);
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"EXECUTE COMPLETED {GetJobCacheKey()}")));
        }

        private void CopyFileCollection(List<IFile> importingFileCollection)
        {
            //prepare file to copy
            importQueueFileCollection = new List<Queue>();
            importFile = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                JobLogID = JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                FileName = "DspReport.json",
                DeliveryFileDate = importingFileCollection.Max(file => file.LastWriteTimeUtc)
            };

            //import pending file
            foreach (IFile incomingFile in importingFileCollection)
            {
                try
                {
                    //check that incomingFile has matching source file
                    CurrentSourceFile = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (CurrentSourceFile == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            PrefixIntegration($"Filename: {incomingFile.Name} skipped because no matching source file found")));
                        continue;
                    }

                    //can we extract datetime from the filename?
                    if (CurrentSourceFile.FileRegexCodec.TryParse(incomingFile.Name))
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"incomingFile.Name: {incomingFile.Name}. sf: {JsonConvert.SerializeObject(CurrentSourceFile)}")));
                        importFile.FileDate = CurrentSourceFile.FileRegexCodec.FileNameDate.Value;
                        importFile.FileDateHour = CurrentSourceFile.FileRegexCodec.FileNameHour;
                        importFile.EntityID = CurrentSourceFile.FileRegexCodec.EntityId.ToLower();
                    }
                    else
                    {
                        importFile.FileDate = incomingFile.LastWriteTimeUtc;
                    }

                    //copy pending file
                    //basebucket/raw/source/date 
                    string[] paths = new string[] { importFile.EntityID, GetDatedPartition(importFile.FileDate), incomingFile.Name };
                    Uri destUri = RemoteUri.CombineUri(GetDestinationFolder(), paths);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"destUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}")));
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    //When too big for direct copy to S3, must copy locally first then multipart to S3
                    if (incomingFile.Length > S3File.MAX_PUT_SIZE)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"FileLength for file {incomingFile.Name} is {incomingFile.Length}.")));
                        Uri tempDestUri = RemoteUri.CombineUri(new Uri(Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                        FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                        if (!tempDestFile.Directory.Exists)
                        {
                            tempDestFile.Directory.Create();
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"Importing file to file system first: {tempDestUri}")));
                        incomingFile.CopyTo(tempDestFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"Moving to S3 : {destUri}")));

                        Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                        TransferUtility transferUtility = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                        transferUtility.UploadAsync(tempDestFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"TransferUtility S3 URI {destUri} upload complete")));
                        tempDestFile.Delete();
                    }
                    else
                    {
                        incomingFile.CopyTo(destFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixIntegration($"File imported to: {destUri}")));
                    }

                    filesIn++;
                    bytesIn += incomingFile.Length;

                    //Add the completed transfer file collection to the transfer log.
                    importQueueFileCollection.Add(new Queue { SourceFileName = CurrentSourceFile.SourceFileName, FileName = incomingFile.Name, FileSize = incomingFile.Length });
                }
                catch (Exception exc)
                {
                    //make sure we log the failure to the transfer logs                    
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixIntegration($"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length}, Exception was: {exc.Message}")));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixIntegration($"EXECUTE ERROR {GetJobCacheKey()}")));
                    throw;
                }
            }

            AddFileToQueue();
        }

        private void AddFileToQueue()
        {
            try
            {
                //store files to be imported csv of FileType:FilePath.
                var files = importQueueFileCollection.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName,
                    FileSize = x.FileSize
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    PrefixIntegration($"matched {importQueueFileCollection.Count} " +
                    $"files from source using regex: {filesJSON}. Integration date is: {CurrentIntegration.FileStartDate}")));

                importFile.SourceFileName = CurrentSourceFile.SourceFileName;
                importFile.Status = Constants.JobStatus.Complete.ToString();
                importFile.StatusId = (int)Constants.JobStatus.Complete;
                importFile.FileCollectionJSON = filesJSON;
                importFile.FileSize = bytesIn;
                Data.Services.JobService.Add(importFile);

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                    PrefixIntegration($"Successfully queued {CurrentSource.SourceName} dsp report files {filesJSON}.")));
            }
            catch (Exception exc)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixIntegration($"Error queuing {CurrentSource.SourceName} dsp report files {JsonConvert.SerializeObject(importQueueFileCollection)}"), exc));
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
                RAC?.Dispose();
            }
        }
        ~ImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        /// <summary>
        /// Appends Integration to the beginning of the param [message]
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        protected string PrefixIntegration(string message)
        {
            return PrefixJobGuid($"Integration - {CurrentIntegration.IntegrationName} - " + message);
        }
    }
}
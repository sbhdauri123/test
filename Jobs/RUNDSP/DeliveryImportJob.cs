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
using System.IO;
using System.Linq;

namespace Greenhouse.Jobs.RUNDSP
{
    [Export("RUNDSP-DeliveryImportJob", typeof(IDragoJob))]
    public class DeliveryImportJob : BaseFrameworkJob, IDragoJob
    {
        private static Logger logger { get; set; } = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC { get; set; }
        private List<IFile> importingFiles { get; set; } = new List<IFile>();
        private Queue importFile { get; set; }
        private SourceFile CurrentSourceFile { get; set; }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"IMPORT-PREEXECUTE {this.GetJobCacheKey()}"));

            //initialize the appropriate client for this integration
            RAC = GetRemoteAccessClient();
        }
        public void Execute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"EXECUTE START {this.GetJobCacheKey()}")));

            //get files already imported
            var importedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID).Select(fl => new { fl.FileName });

            //get all file path to be imported (potentially)
            List<string> importingFilesStage = GenerateFilePathList();

            //remove files already imported
            importingFilesStage.RemoveAll(remoteFile => importedFiles.Any(imported => imported.FileName.Equals(Path.GetFileName(remoteFile), StringComparison.CurrentCultureIgnoreCase)));
            if (importingFilesStage?.Count < 1)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"All files have already been imported for Integration: {CurrentIntegration.IntegrationName}")));
                return;
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Integration: {CurrentIntegration.IntegrationName}, File Start Date is: {CurrentIntegration.FileStartDate}")));

            //get pending Files
            GetFiles(importingFilesStage);

            string totalBytes = UtilsText.GetFormattedSize((double)importingFiles.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid($"Integration: {base.CurrentIntegration.IntegrationName}, Source Files ({RAC.GetType().ToString()}): {importingFiles.Count}, Destination Files (S3): {importedFiles.Count()} Preparing: {importingFiles.Count} files for import. {totalBytes} total bytes.")));

            //import pending files
            ImportFiles();
        }

        /// <summary>
        /// Generates the list of file path of files to import
        /// </summary>
        /// <returns></returns>
        private List<string> GenerateFilePathList()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Start: Generating file path")));

            List<string> filePathList = new List<string>();
            DateTime startDate = CurrentIntegration.FileStartDate;
            DateTime endDate = DateTime.UtcNow;

            int dateDiffDays = (endDate - startDate).Days;
            int dateDiffHours = (endDate - startDate).Hours;

            dateDiffDays += (dateDiffHours > 0) ? 1 : 0;

            foreach (var sourceFile in SourceFiles)
            {
                //reset start date
                startDate = CurrentIntegration.FileStartDate;
                for (int d = 1; d <= dateDiffDays; d++)
                {
                    //Ensures partial day to be build (when less than 24 hours)
                    int hour = ((dateDiffDays - d) == 0) ? dateDiffHours : 24;
                    for (int h = 0; h < hour; h++)
                    {
                        filePathList.Add($"{CurrentIntegration.EndpointURI}/{sourceFile.SourceFileName}/{startDate.ToString("yyyy/MM/dd/HH")}/pixeletl_{sourceFile.SourceFileName}-{startDate.ToString("yyyyMMddHH")}.json");
                        startDate = startDate.AddHours(1);
                    }
                }
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Completed: Generating file path")));

            return filePathList;
        }
        private void GetFiles(List<string> importingFilesStage)
        {
            foreach (string filename in importingFilesStage)
            {
                var s3SrcFile = RAC.WithFile(new Uri(filename));
                if (s3SrcFile.Exists)
                {
                    importingFiles.Add(s3SrcFile);
                }
                else
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        PrefixJobGuid($"Filename: {filename} skipped because no matching S3 file found")));
                }
            }
        }
        private void ImportFiles()
        {
            foreach (IFile incomingFile in importingFiles)
            {
                try
                {
                    //check that incomingFile has matching source file
                    CurrentSourceFile = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (CurrentSourceFile == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            PrefixJobGuid($"Filename: {incomingFile.Name} skipped because no matching source file found")));
                        continue;
                    }

                    //prepare file to copy
                    PrepareFileToCopy(incomingFile);

                    //copy pending file
                    CopyFile(incomingFile);

                    //add file to the queue
                    AddFileToQueue();
                }
                catch (Exception exc)
                {
                    //make sure we log the failure to the transfer logs
                    importFile.Status = Constants.JobStatus.Error.ToString();
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid($"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length}, Exception was: {exc.Message}")));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"EXECUTE ERROR {this.GetJobCacheKey()}")));
                    throw;
                }
            }
        }
        private void PrepareFileToCopy(IFile incomingFile)
        {
            importFile = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                FileName = incomingFile.Name,
                FileSize = incomingFile.Length,
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                Status = Constants.JobStatus.Complete.ToString(),
                StatusId = (int)Constants.JobStatus.Complete,
                JobLogID = JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                DeliveryFileDate = incomingFile.LastWriteTimeUtc
            };

            importFile.SourceFileName = CurrentSourceFile.SourceFileName;
            //can we extract datetime from the filename?
            if (CurrentSourceFile.FileRegexCodec.TryParse(incomingFile.Name))
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"incomingFile.Name: {incomingFile.Name}. sf: {JsonConvert.SerializeObject(CurrentSourceFile)}")));
                importFile.FileDate = CurrentSourceFile.FileRegexCodec.FileNameDate.Value;
                importFile.FileDateHour = CurrentSourceFile.FileRegexCodec.FileNameHour;
                importFile.EntityID = CurrentSourceFile.FileRegexCodec.EntityId;
            }
            else
            {
                importFile.FileDate = incomingFile.LastWriteTimeUtc;
            }
        }
        private void CopyFile(IFile incomingFile)
        {
            //basebucket/raw/source/entityid/date 
            string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), importFile.FileName };
            Uri destUri = RemoteUri.CombineUri(GetDestinationFolder(), paths);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"destUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}")));
            S3File destFile = new S3File(destUri, GreenhouseS3Creds);

            //When too big for direct copy to S3, must copy locally first then multipart to S3
            if (incomingFile.Length > S3File.MAX_PUT_SIZE)
            {
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"FileLength for file {incomingFile.Name} is {incomingFile.Length}.")));
                Uri tempDestUri = RemoteUri.CombineUri(new Uri(Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                if (!tempDestFile.Directory.Exists)
                {
                    tempDestFile.Directory.Create();
                }

                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"Importing file to file system first: {tempDestUri}")));
                incomingFile.CopyTo(tempDestFile, true);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"Moving to S3 : {destUri}")));

                Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                TransferUtility transferUtility = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                transferUtility.UploadAsync(tempDestFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"TransferUtility S3 URI {destUri} upload complete")));
                tempDestFile.Delete();
            }
            else
            {
                incomingFile.CopyTo(destFile, true);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"File imported to: {destUri}")));
            }

            filesIn++;
            bytesIn += incomingFile.Length;
        }
        private void AddFileToQueue()
        {
            //add the completed transfer record to the transfer log
            importFile.Status = Constants.JobStatus.Complete.ToString();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Adding to queue: {JsonConvert.SerializeObject(importFile)}")));
            Data.Services.JobService.Add(importFile);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"EXECUTE END {this.GetJobCacheKey()}")));
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

        ~DeliveryImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }
    }
}
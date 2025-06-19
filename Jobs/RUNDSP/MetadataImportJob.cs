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

namespace Greenhouse.Jobs.RUNDSP
{
    [Export("RUNDSP-MetadataImportJob", typeof(IDragoJob))]
    public class MetadataImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC;
        private Uri _baseDestUri;
        private Queue _importFile;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            _baseDestUri = GetDestinationFolder();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"IMPORT-PREEXECUTE {this.GetJobCacheKey()}")));
        }

        public void Execute()
        {
            logger.Log(
                Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"EXECUTE START {this.GetJobCacheKey()}")));

            //check if already imported today's files
            var importedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
            DateTime latestImportedFileDate = (importedFiles == null || !importedFiles.Any())
                ? CurrentIntegration.FileStartDate
                : importedFiles.Max(x => x.FileDate);
            var offsetDays = (CurrentSource.DeliveryOffset == null) ? 1 : CurrentSource.DeliveryOffset.Value;
            var fileDate = DateTime.Today.AddDays(-1 * offsetDays).ToUniversalTime();
            if (latestImportedFileDate.Subtract(fileDate).Days == 0)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    PrefixJobGuid(
                        $"Integration: {CurrentIntegration.IntegrationName} has already been imported for file date: {fileDate}")));
                return;
            }

            List<IFile> importFilesBySourceFilename = new List<IFile>();
            RAC = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Integration: {CurrentIntegration.IntegrationName}, fetching source files against regex: {regCod.FileNameRegex}. File Start Date: {CurrentIntegration.FileStartDate}")));

            foreach (SourceFile sourceFile in SourceFiles)
            {
                var runVersion = sourceFile.SourceFileName.Split('-')[0].ToLower();
                var metadataType = sourceFile.SourceFileName.Split('-')[1].ToLower();
                var latestFolder = Data.Services.SetupService.GetById<Lookup>(Constants.RUNDSP_METADATA_FOLDER_NAME).Value;
                string[] paths = new string[] { runVersion, latestFolder, metadataType };
                Uri sourceUri = RemoteUri.CombineUri(new Uri(CurrentIntegration.EndpointURI), paths);
                var metadataFiles = RAC.WithDirectory(sourceUri).GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) &&
                    regCod.FileNameDate.Value.Date == fileDate.Date).ToList();

                if (metadataFiles.Count != 0)
                {
                    importFilesBySourceFilename.Add(metadataFiles.OrderByDescending(file => file.LastWriteTimeUtc).FirstOrDefault());
                }
            }

            var importingFiles = importFilesBySourceFilename.Select(f => new { f.FullName, f.LastWriteTimeUtc });
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Integration: {CurrentIntegration.IntegrationName}, matched {importFilesBySourceFilename.Count} files from source using regex: {JsonConvert.SerializeObject(importingFiles)}. File Date is: {fileDate}")));

            //We need to account for duplicate file types, check that we have all distinct file types. 
            var srcFileCount = SourceFiles.Count();
            if (importFilesBySourceFilename.Count != srcFileCount)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    PrefixJobGuid(
                        $"Integration: {CurrentIntegration.IntegrationName}, mismatch with number of files to be imported. It will try to import {importFilesBySourceFilename.Count} of {srcFileCount} source files. File Date is: {fileDate}")));
                return;
            }

            string totalBytes =
                Greenhouse.Utilities.UtilsText.GetFormattedSize((double)importFilesBySourceFilename.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                PrefixJobGuid(
                    $"Integration: {base.CurrentIntegration.IntegrationName} preparing {importFilesBySourceFilename.Count} files for import - {totalBytes} total bytes.")));

            _importFile = new Queue()
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
                FileDate = fileDate
            };

            List<Queue> downloadedFiles = new List<Queue>();
            //Download files to be imported
            foreach (IFile incomingFile in importFilesBySourceFilename)
            {
                try
                {
                    SourceFile sf =
                        SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (string.IsNullOrEmpty(_importFile.EntityID))
                    {
                        var match = sf.FileRegexCodec.FileNameRegex.Match(incomingFile.Name).Groups;
                        _importFile.EntityID = match[Constants.REGEX_ENTITYID].Value;
                    }

                    //basebucket/raw/source/entityid/date 
                    string[] paths = new string[]
                        {_importFile.EntityID.ToLower(), GetDatedPartition(_importFile.FileDate), incomingFile.Name};
                    Uri destUri = RemoteUri.CombineUri(this._baseDestUri, paths);
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    //too big for direct copy to S3, must copy locally first then multipart to S3
                    if (incomingFile.Length > S3File.MAX_PUT_SIZE)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            PrefixJobGuid($"FileLength for file {incomingFile.Name} is {incomingFile.Length}.")));
                        Uri tempDestUri = RemoteUri.CombineUri(
                            new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                        FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                        if (!tempDestFile.Directory.Exists)
                        {
                            tempDestFile.Directory.Create();
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            PrefixJobGuid($"Importing file to file system first: {tempDestUri}")));
                        incomingFile.CopyTo(tempDestFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, PrefixJobGuid($"Moving to S3 : {destUri}")));

                        Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                        TransferUtility tu = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                        tu.UploadAsync(tempDestFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            PrefixJobGuid($"TransferUtility S3 URI {destUri} upload complete")));
                        tempDestFile.Delete();
                    }
                    else
                    {
                        incomingFile.CopyTo(destFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            PrefixJobGuid($"File imported to: {destUri}")));
                    }

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    //add the completed transfer record to the transfer log
                    downloadedFiles.Add(new Queue
                    {
                        SourceFileName = sf.SourceFileName,
                        FileName = incomingFile.Name,
                        FileSize = incomingFile.Length
                    });
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        PrefixJobGuid(
                            $"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length}, Exception was: {exc.Message}")));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        PrefixJobGuid($"EXECUTE ERROR {this.GetJobCacheKey()}")));
                    throw;
                }
            }

            QueueImportFiles(downloadedFiles);
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

        ~MetadataImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        public void QueueImportFiles(List<Queue> downloadedFiles)
        {
            try
            {
                //store files to be imported csv of FileType:FilePath
                var files = downloadedFiles.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName,
                    FileSize = x.FileSize
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                _importFile.FileName = "metadata.json";
                _importFile.Status = Common.Constants.JobStatus.Complete.ToString();
                _importFile.StatusId = (int)Constants.JobStatus.Complete;
                _importFile.FileCollectionJSON = filesJSON;
                _importFile.FileSize = bytesIn;
                Data.Services.JobService.Add(_importFile);
                string msg = PrefixJobGuid($"Successfully queued RUN DSP metadata files {filesJSON}.");
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, msg));
            }
            catch (Exception exc)
            {
                string errMsg = PrefixJobGuid($"Error queuing RUN DSP metadata files {JsonConvert.SerializeObject(downloadedFiles)}");
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, exc));
                throw;
            }
        }
    }
}

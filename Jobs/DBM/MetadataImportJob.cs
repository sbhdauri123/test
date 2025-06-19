using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
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

namespace Greenhouse.Jobs.DBM
{
    [Export("DBM-PublicMetadataImportJob", typeof(IDragoJob))]
    [Export("DBM-PrivateMetadataImportJob", typeof(IDragoJob))]
    public partial class ImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static Logger logger { get; set; } = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient RAC { get; set; }
        private Queue importFile { get; set; }
        private Uri baseDestUri { get; set; }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
        }

        /// <summary>
        /// Note: DBM files are not one day behind like other sources.
        /// </summary>
        public void Execute()
        {
            //check if already imported today's files
            var importedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            #region Get latest imported file date
            DateTime latestImportedFileDate = (importedFiles == null || !importedFiles.Any()) ? CurrentIntegration.FileStartDate : importedFiles.Max(x => x.FileDate);
            var fileDate = DateTime.Today.ToUniversalTime();
            if (latestImportedFileDate.Subtract(fileDate).Days == 0)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Integration: {CurrentIntegration.IntegrationName} has already been imported for file date: {fileDate}")));
                return;
            }
            #endregion

            //initialize the appropriate client for this integration
            RAC = GetRemoteAccessClient();
            RegexCodec integrationRegex = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Integration: {CurrentIntegration.IntegrationName}, fetching source files against regex: {integrationRegex.FileNameRegex}. File Start Date is: {fileDate}")));

            // Grab files that need to be imported today
            string strFileDate = fileDate.ToString("yyyyMMdd");

            //Filter files by integration's regex mask
            var importFiles = RAC.WithDirectory(new Uri(CurrentIntegration.EndpointURI), HttpClientProvider).GetFiles().Where(f => integrationRegex.FileNameRegex.IsMatch(f.Name) && f.Name.IndexOf(strFileDate) > -1).ToList();

            //Ignore SourceFile regex not setup
            importFiles.RemoveAll(file => !SourceFiles.Any(source => source.FileRegexCodec.FileNameRegex.IsMatch(file.Name)));

            //Group files by SourceFilename
            var importFileGroupBySourceFilename =
                importFiles.GroupBy(file => SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(file.Name)).SourceFileName)
                .ToDictionary(sourceFilename => sourceFilename.Key, fileList => fileList);

            //When multiple files per source type yield only one file per source type with max LastWriteTimeUtc
            var importingFilesDistinct = importFileGroupBySourceFilename.Select(fileList => fileList.Value.OrderByDescending(file => file.LastWriteTimeUtc).FirstOrDefault());

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Integration: {CurrentIntegration.IntegrationName}, fetched {importingFilesDistinct.Count()} files against regex: {JsonConvert.SerializeObject(importingFilesDistinct)}. File Start Date is: {fileDate}")));

            //We need to account for duplicate file types, check that we have all distinct file types. 
            var srcFileCount = SourceFiles.Count();

            if (importingFilesDistinct.Count() != srcFileCount)
            {
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid($"Integration: {CurrentIntegration.IntegrationName}, fetched {importingFilesDistinct.Count()} files against regex: {JsonConvert.SerializeObject(importingFilesDistinct)}. {srcFileCount} source files expected. File Start Date is: {fileDate}")));
                return;
            }

            string totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)importingFilesDistinct.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Integration: {base.CurrentIntegration.IntegrationName}, Source Files ({RAC.GetType().ToString()}): {importingFilesDistinct.Count()}, Destination Files (S3): {importedFiles.Count()} Preparing: {importingFilesDistinct.Count()} files for import. {totalBytes} total bytes.")));

            int hour = 0;

            var entity = EndpointUriRegex().Match(CurrentIntegration.EndpointURI);
            string entityID = string.Empty;

            if (!entity.Success)
            {
                throw new RegexException(PrefixJobGuid($"Failed to resolve EntityID from Integration's ({CurrentIntegration.IntegrationID}) EndPoint -> {CurrentIntegration.EndpointURI}"));
            }

            entityID = entity.Groups[1].Value;
            importFile = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                FileName = PrefixJobGuid($"DBM Metadata ({entity})"),
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                Status = Constants.JobStatus.Pending.ToString(),
                StatusId = (int)Constants.JobStatus.Running,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                SourceFileName = "Metadata",
                FileDateHour = hour,
                FileDate = fileDate,
                EntityID = entityID
            };

            List<Queue> downloadedFiles = new List<Queue>();

            //Download files to be imported
            foreach (IFile incomingFile in importingFilesDistinct)
            {
                try
                {
                    //basebucket/raw/source/entityid/date 
                    string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), incomingFile.Name };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    base.UploadToS3(incomingFile, (S3File)destFile, paths);

                    filesIn++;
                    bytesIn += incomingFile.Length;

                    SourceFile sf = SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    //add the completed transfer record to the transfer log	
                    downloadedFiles.Add(new Queue { SourceFileName = sf.SourceFileName, FileName = incomingFile.Name, FileSize = incomingFile.Length });
                }
                catch (Exception exc)
                {
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, PrefixJobGuid($"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length}, Exception was: {exc.Message}")));
                    throw;
                }
            }

            QueueImportFiles(downloadedFiles);
        }

        public void QueueImportFiles(List<Queue> downloadedFiles)
        {
            try
            {
                //store files to be imported csv of FileType:FilePath
                var files = downloadedFiles.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                importFile.FileName = "metadata.json";
                importFile.Status = Common.Constants.JobStatus.Complete.ToString();
                importFile.StatusId = (int)Constants.JobStatus.Complete;
                importFile.FileCollectionJSON = filesJSON;
                importFile.FileSize = bytesIn;
                Data.Services.JobService.Add(importFile);
                string msg = PrefixJobGuid($"Successfully queued {CurrentIntegration.IntegrationName} metadata files {filesJSON}.");
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, msg));
            }
            catch (Exception exc)
            {
                string errMsg = PrefixJobGuid($"Error queuing {CurrentIntegration.IntegrationName} files {JsonConvert.SerializeObject(downloadedFiles)}");
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

        [GeneratedRegex(".*-(.*)/entity")]
        private static partial Regex EndpointUriRegex();
    }
}
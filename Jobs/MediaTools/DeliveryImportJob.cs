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

namespace Greenhouse.Jobs.MediaTools
{
    [Export("MediaTools-DeliveryImportJob", typeof(IDragoJob))]
    public class DeliveryImportJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private RemoteAccessClient RAC;

        private List<IFile> _importFiles;
        private List<IFile> _whatsMissing;
        private Uri baseDestUri;

        private string JobGuid => base.JED.JobGUID.ToString();

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format("{2} - {1}-IMPORT-PREEXECUTE {0}", this.GetJobCacheKey(), this.CurrentSource, JobGuid)));
        }

        public void Execute()
        {
            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format("{1} - EXECUTE START {0}", this.GetJobCacheKey(), JobGuid)));
            Queue? importFile = null;

            RAC = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format(
                "{3} - Integration: {0}, fetching source files against regex: {1}. File Start Date is: {2}",
                CurrentIntegration.IntegrationName, regCod.FileNameRegex, CurrentIntegration.FileStartDate, JobGuid)));
            _importFiles = RAC.WithDirectory().GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && regCod.TryParse(f.Name) &&
                    regCod.FileNameDate >= CurrentIntegration.FileStartDate)
                .ToList();

            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            _whatsMissing = _importFiles.Where(x => !processedFiles.Any(y => y.FileName.Equals(x.Name)))
                .OrderBy(p => p.LastWriteTimeUtc).ToList();

            string totalBytes =
                Greenhouse.Utilities.UtilsText.GetFormattedSize((double)_whatsMissing.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format(
                    "{7} - Integration: {0}, Source Files ({1}): {2}, Destination Files ({3}): {4} Preparing: {5} files for import. {6} total bytes.",
                    CurrentIntegration.IntegrationName,
                    RAC.GetType(),
                    _importFiles.Count,
                    "S3",
                    processedFiles.Count(),
                    _whatsMissing.Count,
                    totalBytes,
                    JobGuid)));

            foreach (IFile incomingFile in _whatsMissing)
            {
                try
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
                        JobLogID = this.JobLogger.JobLog.JobLogID,
                        Step = JED.Step.ToString(),
                        DeliveryFileDate = incomingFile.LastWriteTimeUtc
                    };
                    SourceFile sf = base.SourceFiles.SingleOrDefault(s =>
                        s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));

                    if (sf == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format("{1} - Filename: {0} skipped because no matching source file found",
                                incomingFile.Name, JobGuid)));
                        continue; //do we want to record this somewhere? 
                    }

                    importFile.SourceFileName = sf.SourceFileName;
                    if (sf.FileRegexCodec.TryParse(incomingFile.Name))
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{2} - incomingFile.Name: {1}. sf: {0}", JsonConvert.SerializeObject(sf),
                                incomingFile.Name, JobGuid)));
                        importFile.FileDate = sf.FileRegexCodec.FileNameDate.Value;
                        importFile.FileDateHour = sf.FileRegexCodec.FileNameHour;
                        importFile.EntityID = sf.FileRegexCodec.EntityId;
                    }
                    else
                    {
                        importFile.FileDate = incomingFile.LastWriteTimeUtc;
                    }

                    string[] paths = new string[]
                        {importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), importFile.FileName};
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                        String.Format("{2} - destUri: {0}. paths: {1} ", JsonConvert.SerializeObject(destUri),
                            JsonConvert.SerializeObject(paths), JobGuid)));
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    if (incomingFile.Length > S3File.MAX_PUT_SIZE)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            String.Format("{0} - FileLength for file {1} is {2}.", JobGuid, incomingFile.Name,
                                incomingFile.Length)));
                        Uri tempDestUri = RemoteUri.CombineUri(
                            new Uri(Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseImportPath), paths);
                        FileSystemFile tempDestFile = new FileSystemFile(tempDestUri);

                        if (!tempDestFile.Directory.Exists)
                        {
                            tempDestFile.Directory.Create();
                        }

                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{1} - Importing file to file system first: {0}", tempDestUri, JobGuid)));
                        incomingFile.CopyTo(tempDestFile, true);
                        logger.Log(
                            Msg.Create(LogLevel.Debug, logger.Name,
                                string.Format("{1} - Moving to S3 : {0}", destUri, JobGuid)));

                        Amazon.S3.Util.AmazonS3Uri s3Uri = new Amazon.S3.Util.AmazonS3Uri(destUri);
                        TransferUtility tu = GetMultipartTransferUtility(Configuration.Settings.Current.AWS.Region);
                        tu.UploadAsync(tempDestFile.FullName, s3Uri.Bucket, s3Uri.Key).GetAwaiter().GetResult();
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                            string.Format("{1} - TransferUtility S3 URI {0} upload complete", destUri, JobGuid)));
                        tempDestFile.Delete();
                    }
                    else
                    {
                        incomingFile.CopyTo(destFile, true);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                            string.Format("{1} - File imported to: {0}", destUri, JobGuid)));
                    }

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    importFile.Status = Common.Constants.JobStatus.Complete.ToString();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{1} - Adding to queue: {0}", JsonConvert.SerializeObject(importFile), JobGuid)));
                    Data.Services.JobService.Add(importFile);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{1} - EXECUTE END {0}", this.GetJobCacheKey(), JobGuid)));
                }
                catch (Exception exc)
                {
                    importFile.Status = Common.Constants.JobStatus.Error.ToString();
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name,
                        string.Format("{3} - Import failed on file {0} - Size: {1}, Exception was: {2}",
                            incomingFile.Uri,
                            incomingFile.Length, exc.Message, JobGuid)));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{1} - EXECUTE ERROR {0}", this.GetJobCacheKey(), JobGuid)));
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
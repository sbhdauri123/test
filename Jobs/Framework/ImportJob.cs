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

namespace Greenhouse.Jobs.Framework
{
    [Export("GenericImportJob", typeof(IDragoJob))]
    public class ImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private RemoteAccessClient RAC;

        private List<IFile> _importFiles;

        private Uri baseDestUri;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("IMPORT-PREEXECUTE {0}", this.GetJobCacheKey())));
        }

        public void Execute()
        {
            List<IFile> whatsMissing;

            string jobGuid = base.JED.JobGUID.ToString();
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE START {0}", this.GetJobCacheKey(), jobGuid)));
            Queue? importFile = null;

            //initialize the appropriate client for this integration
            RAC = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{3} - Integration: {0}, fetching source files against regex: {1}. File Start Date is: {2}",
                CurrentIntegration.IntegrationName, regCod.FileNameRegex, CurrentIntegration.FileStartDate, jobGuid)));
            _importFiles = RAC.WithDirectory(new Uri(CurrentIntegration.EndpointURI), HttpClientProvider).GetFiles().Where(f => regCod.FileNameRegex.IsMatch(f.Name) && f.LastWriteTimeUtc >= CurrentIntegration.FileStartDate).ToList();

            //TO DO - filter it
            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
            whatsMissing = _importFiles.Except(_importFiles.Where(s => processedFiles.Select(p => p.FileName).Contains(s.Name))).OrderBy(p => p.LastWriteTimeUtc).ToList();

            string totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)whatsMissing.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format("{7} - Integration: {0}, Source Files ({1}): {2}, Destination Files ({3}): {4} Preparing: {5} files for import. {6} total bytes.",
                    base.CurrentIntegration.IntegrationName,
                    RAC.GetType().ToString(),
                    _importFiles.Count,
                    "S3",
                    processedFiles.Count(),
                    whatsMissing.Count,
                    totalBytes, jobGuid)));

            foreach (IFile incomingFile in whatsMissing)
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
                    SourceFile sf = base.SourceFiles.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));

                    if (sf == null)
                    {
                        logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Filename: {0} skipped because no matching source file found", incomingFile.Name, jobGuid)));
                        continue; //do we want to record this somewhere? 
                    }
                    importFile.SourceFileName = sf.SourceFileName;
                    //can we extract datetime from the filename?
                    if (sf.FileRegexCodec.TryParse(incomingFile.Name))
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{2} - incomingFile.Name: {1}. sf: {0}", JsonConvert.SerializeObject(sf), incomingFile.Name, jobGuid)));
                        importFile.FileDate = sf.FileRegexCodec.FileNameDate.Value;
                        importFile.FileDateHour = sf.FileRegexCodec.FileNameHour;
                        importFile.EntityID = sf.FileRegexCodec.EntityId;
                    }
                    //we cannot, use the server datetime for it (this is bad and should never happen but I'm just the architect, wtf do I know?) 
                    else
                    {
                        importFile.FileDate = incomingFile.LastWriteTimeUtc;
                    }

                    //basebucket/raw/source/entityid/date 
                    string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), importFile.FileName };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, String.Format("{2} - destUri: {0}. paths: {1} ", JsonConvert.SerializeObject(destUri), JsonConvert.SerializeObject(paths), jobGuid)));
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    base.UploadToS3(incomingFile, (S3File)destFile, paths);

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    //add the completed transfer record to the transfer log
                    importFile.Status = Common.Constants.JobStatus.Complete.ToString();
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Adding to queue: {0}", JsonConvert.SerializeObject(importFile), jobGuid)));
                    Data.Services.JobService.Add(importFile);
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE END {0}", this.GetJobCacheKey(), jobGuid)));
                }
                catch (Exception exc)
                {
                    //make sure we log the failure to the transfer logs                    
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, exc));
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{3} - Import failed on file {0} - Size: {1}, Exception was: {2}", incomingFile.Uri, incomingFile.Length, exc.Message, jobGuid)));
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - EXECUTE ERROR {0}", this.GetJobCacheKey(), jobGuid)));
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

        ~ImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }
    }
}

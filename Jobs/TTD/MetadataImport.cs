using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
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

namespace Greenhouse.Jobs.TTD
{
    [Export("TTD-MetadataImportJob", typeof(IDragoJob))]
    public class MetadataImport : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private Queue importFile;
        private List<IFile> _importFiles;

        private List<Queue> _downloadedFiles { get; set; }
        private Uri baseDestUri;

        public IReadOnlyList<Queue> DownloadedFiles { get { return _downloadedFiles.AsReadOnly(); } }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            baseDestUri = GetDestinationFolder();
        }

        public void Execute()
        {
            //TTD dimension files are located at thetradedesk-useast-partner-datafeed/publicis_zenith/idmapping/ykzufg0.
            if (!ShouldImportTheFiles())
            {
                return;
            }

            //We want all the files from the integration source directory; no regex matching needed.
            string totalBytes = Greenhouse.Utilities.UtilsText.GetFormattedSize((double)_importFiles.Sum(s => s.Length));

            logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                string.Format("{0} - Integration: {1}, # of Source Files: {2}. {3} total bytes.", base.JED.TriggerName, base.CurrentIntegration.IntegrationName
                                                                                                    , _importFiles.Count, totalBytes)));

            var endpointParts = base.CurrentIntegration.EndpointURI.Split('/');
            var entityId = endpointParts.Last();

            importFile = new Queue()
            {
                FileGUID = Guid.NewGuid(),
                FileName = "Metadata",
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                Status = Constants.JobStatus.Complete.ToString(),
                StatusId = (int)Constants.JobStatus.Complete,
                JobLogID = this.JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                SourceFileName = "Metadata",
                FileDateHour = 0,
                EntityID = entityId
            };

            foreach (IFile incomingFile in _importFiles)
            {
                try
                {
                    importFile.FileDate = incomingFile.LastWriteTimeUtc;
                    //basebucket/raw/source/entityid/date 
                    string[] paths = new string[] { importFile.EntityID.ToLower(), GetDatedPartition(importFile.FileDate), incomingFile.Name };
                    Uri destUri = RemoteUri.CombineUri(this.baseDestUri, paths);
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);
                    incomingFile.CopyTo(destFile, true);
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{0} - File imported to: {1}", base.JED.TriggerName, destUri)));

                    filesIn++;
                    bytesIn += incomingFile.Length;
                    //add the completed transfer record to the transfer log				

                    if (_downloadedFiles == null)
                        _downloadedFiles = new List<Queue>();
                    var fileType = incomingFile.Name.Split('.')[0];
                    _downloadedFiles.Add(new Queue { SourceFileName = fileType, FileName = incomingFile.Name, FileSize = incomingFile.Length });
                }
                catch (Exception exc)
                {
                    //base.AddStepMetric(Greenhouse.Data.Model.Core.StepMetric.MetricType.NumberOfFiles, filesIn);
                    //base.AddStepMetric(Greenhouse.Data.Model.Core.StepMetric.MetricType.FileSize, bytesIn);
                    logger.Log(Msg.Create(LogLevel.Error, logger.Name, string.Format("{0} - Import failed on file {1} - Size: {2}, Exception was: {3}", base.JED.TriggerName, incomingFile.Uri, incomingFile.Length, exc.Message)));
                    throw;
                }
            }

            QueueImportFiles();

            //base.AddStepMetric(Greenhouse.Data.Model.Core.StepMetric.MetricType.NumberOfFiles, filesIn);
            //base.AddStepMetric(Greenhouse.Data.Model.Core.StepMetric.MetricType.FileSize, bytesIn);
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

            }
        }

        ~MetadataImport()
        {
            Dispose(false);
        }

        public bool ShouldImportTheFiles()
        {
            bool importFiles = false;

            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
            DateTime latestProcessedFileDate;
            if (processedFiles == null || !processedFiles.Any())
            {
                latestProcessedFileDate = CurrentIntegration.FileStartDate;
            }
            else
            {
                latestProcessedFileDate = processedFiles.Max(x => x.FileDate);
            }

            DateTime lastUpdatedFileDate;
            if (!FileUpdatePending(out lastUpdatedFileDate))
            {
                importFiles = (lastUpdatedFileDate.Date != latestProcessedFileDate.Date);
            }

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, String.Format("{0} - ShouldImportTheFiles returned {1}. lastUpdatedFileDate: {2}; latestProcessedFileDate: {3}", base.JED.TriggerName
                , importFiles, lastUpdatedFileDate, latestProcessedFileDate)));

            return importFiles;
        }

        public bool FileUpdatePending(out DateTime lastUpdatedFileDate)
        {
            var RAC = base.GetRemoteAccessClient();
            _importFiles = RAC.WithDirectory().GetFiles().ToList();
            var dates = _importFiles.GroupBy(x => x.LastWriteTimeUtc.Date).Select(y => y.FirstOrDefault());

            bool pendingUpdate = true;
            lastUpdatedFileDate = DateTime.MinValue;
            if (dates.Count() == 1)
            {
                pendingUpdate = false;
                lastUpdatedFileDate = dates.ToList()[0].LastWriteTimeUtc;
            }
            return pendingUpdate;
        }

        public void QueueImportFiles()
        {
            try
            {
                var files = this.DownloadedFiles.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName //this is basically file type - AdGroup, Application, Audience etc.
                });

                var filesJSON = JsonConvert.SerializeObject(files);

                importFile.FileName = "metadata.json";
                importFile.Status = Greenhouse.Common.Constants.JobStatus.Complete.ToString();
                importFile.StatusId = (int)Constants.JobStatus.Complete;
                importFile.FileCollectionJSON = filesJSON;
                importFile.FileSize = bytesIn;
                Data.Services.JobService.Add(importFile);
                string msg = string.Format("{0} - Successfully queued TTD metadata files {1}.", base.JED.TriggerName, filesJSON);
                logger.Log(Msg.Create(LogLevel.Debug, logger.Name, msg));
            }
            catch (Exception exc)
            {
                string errMsg = string.Format("{0} - Error queuing TTD metadata files {1}", base.JED.TriggerName, JsonConvert.SerializeObject(this._downloadedFiles));
                logger.Log(Msg.Create(LogLevel.Error, logger.Name, errMsg, exc));
                throw;
            }
        }
    }
}

using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.DBM.Delivery
{
    [Export("DBM-DeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();
        protected List<string> processedFolders;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            Initialize();

            processedFolders = null;
            string processedFoldersValue = Data.Services.SetupService.GetById<Lookup>(Constants.PROCESSED_FOLDERS_FOR_CLEANUP)?.Value;
            if (processedFoldersValue != null)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Processed folders: Lookup value for '{Constants.PROCESSED_FOLDERS_FOR_CLEANUP}':{processedFoldersValue}"));
                processedFolders = JsonConvert.DeserializeObject<List<string>>(processedFoldersValue);
            }

            if (processedFolders == null || processedFolders.Count == 0)
            {
                processedFolders = new List<string>();
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Processed folders: No value set in Lookup '{Constants.PROCESSED_FOLDERS_FOR_CLEANUP}'. No files will be deleted from S3 in case of reinstatement"));
            }
        }

        private sealed class QueueRestatment
        {
            public IFileItem NewFile { get; set; }
            public IEnumerable<IFileItem> PreviousFiles { get; set; }
        }

        public void Execute()
        {
            var queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, JobLogger.JobLog.JobLogID)
                .OrderBy(x => x.ID).ToList();
            var country = Data.Services.JobService.GetById<Country>(CurrentIntegration.CountryID);

            var restatments = new List<QueueRestatment>();

            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);
            IOrderedEnumerable<IFileItem> restatedFiles = null;

            if (processedFiles?.Any() == true)
            {
                foreach (var queueItem in queueItems)
                {
                    var previousFiles = processedFiles.Where(x =>
                        queueItem.FileDateHour == x.FileDateHour && queueItem.FileDate.Subtract(x.FileDate).Days == 0 &&
                        queueItem.SourceID == x.SourceID && x.SourceFileName == queueItem.SourceFileName &&
                        queueItem.EntityID == x.EntityID && queueItem.FileName != x.FileName &&
                        queueItem.ID > x.ID);

                    if (previousFiles.Any())
                        restatments.Add(new QueueRestatment
                        {
                            NewFile = queueItem,
                            PreviousFiles = previousFiles
                        });
                }

                restatedFiles = restatments.SelectMany(r => r.PreviousFiles).OrderByDescending(f => f.LastUpdated);

                if (restatedFiles?.Any() == true) DeleteRestatedQueue(queueItems, restatedFiles);
            }

            /* group and remove restated file(s) that are in queue waiting to be processed. 
              * We keep only the latest queueID, because that's how files are imported, and will contain latest file */
            var filesToDelete = queueItems
                .GroupBy(x => new { x.EntityID, x.FileDate, x.FileDateHour, x.SourceFileName })
                .Where(grp => grp.Count() > 1)
                .SelectMany(x => x.OrderByDescending(q => q.ID).Skip(1));
            if (filesToDelete?.Any() == true) DeleteRestatedQueue(queueItems, filesToDelete);

            ProcessFiles(country, queueItems, restatments);

            if (restatedFiles?.Any() == true)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, $"Restated Files: {restatedFiles.Count()} restated files found. Fileguids:{string.Join(",", restatedFiles.Select(f => f.FileGUID))}"));
                DeleteProcessedFiles(restatedFiles, processedFolders);
            }
            else
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, "Restated Files: No restated files found"));
            }
        }

        private void DeleteRestatedQueue(List<IFileItem> queueItems, IEnumerable<IFileItem> restateddFiles)
        {
            UpdateQueueWithDelete(restateddFiles, Constants.JobStatus.Complete, true);
            queueItems.RemoveAll(x => restateddFiles.Any(y => y.ID == x.ID));
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

        ~DataLoadJob()
        {
            Dispose(false);
        }

        private void DeleteProcessedFiles(IEnumerable<IFileItem> currDelQueue, List<string> processedFolders)
        {
            #region delete previously processed fileguid

            if (currDelQueue != null && currDelQueue.Any() && processedFolders.Count != 0)
                foreach (var processedFile in currDelQueue)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Deleting Queue ID: {processedFile.ID}; file:{processedFile.SourceFileName};FileGUID:{processedFile.FileGUID}")));

                    foreach (string folder in processedFolders)
                    {
                        DeleteExistingFileGuid(processedFile, folder);
                    }
                }

            #endregion
        }

        private string[] GetJobParams(Country country, IFileItem queueItem, string manifestFileURI,
            DatabricksETLJob etlJob, string restatedFileGuid)
        {
            //file type is case sensitive. Always starts with Capital letter.
            var cultInfo = new System.Globalization.CultureInfo("en-US", false).TextInfo;
            var fileType = cultInfo.ToTitleCase(queueItem.SourceFileName);

            // src
            var srcFile = SourceFiles.FirstOrDefault(x =>
                x.SourceFileName.Equals(queueItem.SourceFileName, StringComparison.InvariantCultureIgnoreCase));
            var tableName = string.Format(etlJob.DatabricksTableName, queueItem.SourceFileName);
            ;
            var partitionCount = srcFile.PartitionCount?.ToString() ?? "12";
            var partitionColumn = string.IsNullOrEmpty(srcFile.PartitionColumn)
                ? "advertiserid"
                : srcFile.PartitionColumn.ToLower();

            var jobParams = new string[]
            {
                S3Protocol, RootBucket,
                fileType,
                country.CountryName,
                tableName,
                manifestFileURI,
                CurrentIntegration.TimeZoneString,
                partitionColumn,
                partitionCount,
                restatedFileGuid ?? ""
            };

            return jobParams;
        }

        private void ProcessFiles(Country country, IEnumerable<IFileItem> files,
            IEnumerable<QueueRestatment> restatments)
        {
            if (files?.Any() != true)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                    PrefixJobGuid($"No files to process for integration: {CurrentIntegration.IntegrationID}")));
                return;
            }

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

            if (etlJob == null)
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);

            bool isFirstQueueItem = true;

            foreach (var queueItem in files)
            {
                var batch = new[]
                {
                    new
                    {
                        s3Path = GetS3PathHelper(queueItem.EntityID, queueItem.FileDate),
                        fileNames = new[] { new { fileGUID = queueItem.FileGUID, fileName = queueItem.FileName } }
                    }
                };

                var filesJSON = JsonConvert.SerializeObject(new { files = batch });
                var manifestFileURI = StageManifestFile(filesJSON);

                string restatmentGuid = null;
                var restatment = restatments.FirstOrDefault(r => r.NewFile == queueItem);
                if (restatment != null)
                    restatmentGuid = restatment.PreviousFiles.OrderByDescending(f => f.ID).First().FileGUID.ToString();

                var jobParams = GetJobParams(country, queueItem, manifestFileURI, etlJob, restatmentGuid);

                var msg = PrefixJobGuid(
                    $"Submitting spark job for integration: {CurrentIntegration.IntegrationID};source: {queueItem.SourceFileName}; with parameters {JsonConvert.SerializeObject(jobParams)}");

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));

                // Update queue to running status                
                Data.Services.JobService.UpdateQueueStatus(queueItem.ID, Constants.JobStatus.Running);

                var job = Task.Run(async () =>
                    await SubmitSparkJobDatabricks(etlJob.DatabricksJobID, queueItem, isFirstQueueItem, true, false, jobParams));

                job.Wait();

                //If job failed, then throw exception
                if (job.Result != ResultState.SUCCESS)
                {
                    //We do not know which FileGUID caused the error, so to ensure that we don't get duplicate data. 
                    DeleteProcessedFiles(new List<IFileItem>() { queueItem }, processedFolders);
                    var errMessage =
                        PrefixJobGuid(
                            $"ERROR->Spark job queue id: {queueItem.ID} returned job status: {job.Result.ToString()}");
                    throw new DatabricksResultNotSuccessfulException(errMessage);
                }
                else
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        PrefixJobGuid(
                            $"SUCCESS->Spark job for integration: {CurrentIntegration.IntegrationID};queue id: {queueItem.ID}; source: {queueItem.SourceFileName}; Summary: {job.Result.ToString()}")));
                }

                isFirstQueueItem = false;
            } //end foreach
        }

        private string StageManifestFile(string filesJSON)
        {
            var RAC = GetS3RemoteAccessClient();

            Stage = Constants.ProcessingStage.STAGE;
            var paths = new string[] { CurrentIntegration.IntegrationID.ToString(), "manifest.json" };
            var fileName = Utilities.RemoteUri.CombineUri(GetDestinationFolder(), paths);
            var rawFile = RAC.WithFile(fileName);

            if (rawFile.Exists)
                rawFile.Delete();

            var byteData = System.Text.Encoding.UTF8.GetBytes(filesJSON);
            using (MemoryStream stream = new MemoryStream(byteData))
            {
                rawFile.Put(stream);
            }

            Stage = Constants.ProcessingStage.RAW;
            //stage/dbm-delivery/id/manifest.json
            return
                $"{Constants.ProcessingStage.STAGE.ToString().ToLower()}/{CurrentSource.SourceName.ToLower()}/{CurrentIntegration.IntegrationID.ToString()}/manifest.json";
        }
    }
}
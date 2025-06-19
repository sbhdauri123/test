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
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.DCM.Delivery
{
    [Export("DCM-DeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        protected List<string> processedFolders;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));

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

        private void DeleteProcessedFiles(IEnumerable<IFileItem> currDelQueue, List<string> processedFolders)
        {
            #region delete previously processed fileguid			
            if (currDelQueue?.Any() == true && processedFolders.Count != 0)
            {
                foreach (var processedFile in currDelQueue)
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"Deleting Queue ID: {processedFile.ID}; file:{processedFile.SourceFileName};FileGUID:{processedFile.FileGUID}")));

                    foreach (string folder in processedFolders)
                    {
                        DeleteExistingFileGuid(processedFile, folder);
                    }
                }
            }
            #endregion
        }

        private sealed class QueueRestatment
        {
            public IFileItem NewFile { get; set; }
            public IEnumerable<IFileItem> PreviousFiles { get; set; }
        }

        public void Execute()
        {
            var queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(x => x.ID).ToList();
            Country country = Data.Services.JobService.GetById<Country>(CurrentIntegration.CountryID);

            // Check if file has already been processed. If processed, then remove directory from s3 and reprocess.
            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            IOrderedEnumerable<IFileItem> restatedFiles = null;
            var restatments = new List<QueueRestatment>();

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
                    {
                        restatments.Add(new QueueRestatment
                        {
                            NewFile = queueItem,
                            PreviousFiles = previousFiles
                        });
                    }
                }

                restatedFiles = restatments.SelectMany(r => r.PreviousFiles).OrderByDescending(f => f.LastUpdated);

                if (restatedFiles?.Any() == true)
                {
                    DeleteRestatedQueue(queueItems, restatedFiles);
                }
            }

            /* group and remove restated file(s) that are in queue waiting to be processed. 
             * We keep only the latest queueID, because that's how files are imported, and will contain latest file */
            var filesToDelete = queueItems.GroupBy(x => new { x.EntityID, x.FileDate, x.FileDateHour, x.SourceFileName })
                                .Where(grp => grp.Count() > 1)
                                .SelectMany(x => x.OrderByDescending(q => q.ID).Skip(1));
            if (filesToDelete?.Any() == true)
            {
                DeleteRestatedQueue(queueItems, filesToDelete);
            }

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

        #region Databricks
        /// <summary>
        /// Processes all other files types that are not click files
        /// </summary>
        /// <param name="isFirstItem"></param>
        /// <param name="queueItems"></param>
        private void ProcessFiles(Country country, IEnumerable<IFileItem> queueItems, IEnumerable<QueueRestatment> restatments)
        {
            if (queueItems?.Any() != true)
            {
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"ProcessFiles()=>queueItems empty.")));
                return;
            }

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobBySourceID(CurrentSource.SourceID);

            if (etlJob == null)
            {
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
            }

            bool isFirstQueueItem = true;

            foreach (var queueItem in queueItems)
            {
                string restatmentGuid = null;
                var restatment = restatments.FirstOrDefault(r => r.NewFile == queueItem);
                if (restatment != null)
                {
                    restatmentGuid = restatment.PreviousFiles.OrderByDescending(f => f.ID).First().FileGUID.ToString();
                }

                //job paramaters are order specific: s3Protocol, s3RootBucket, fileType, agency, country, fileGUID, rawFilePath, timeZone, partitionColumn, partitions
                //raw filepath is the FULL path to the file. i.e. s3a://presto-test-123/rawtest3/dcm_floodlight685973_impression_2016041500_20160415_033815_236076527.csv.gz			
                var jobParams = GetJobParams(country, queueItem, string.Empty, etlJob, restatmentGuid);

                var msg = PrefixJobGuid($"Submitting spark job for integration: {CurrentIntegration.IntegrationID};source: {queueItem.SourceFileName}; with parameters {JsonConvert.SerializeObject(jobParams)}");

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));

                var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID, queueItem, isFirstQueueItem, true, false, jobParams));
                job.Wait();

                //If job failed, then throw exception
                if (job.Result != ResultState.SUCCESS)
                {
                    DeleteProcessedFiles(new List<IFileItem>() { queueItem }, processedFolders);
                    string errMessage = PrefixJobGuid($"ERROR->Spark job for queue id: {queueItem.ID} returned job status: {job.Result.ToString()}");
                    throw new DatabricksResultNotSuccessfulException(errMessage);
                }
                else
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid($"SUCCESS->Spark job for integration: {CurrentIntegration.IntegrationID};queue id: {queueItem.ID}; source: {queueItem.SourceFileName}; Summary: {job.Result.ToString()}")));
                }

                isFirstQueueItem = false;
            }
        }

        private string[] GetJobParams(Country country, IFileItem queueItem, string filesJSON, DatabricksETLJob etlJob, string restatedFileGuid)
        {
            //file type is case sensitive. Always starts with Capital letter.
            var cultInfo = new System.Globalization.CultureInfo("en-US", false).TextInfo;
            string fileType = cultInfo.ToTitleCase(queueItem.SourceFileName);

            // src path needs to be json 
            if (string.IsNullOrEmpty(filesJSON))
            {
                var srcFileUri = GetS3PathHelper(queueItem.EntityID, queueItem.FileDate);

                var obj = new
                {
                    files = new[] {
                        new {
                            s3Path = srcFileUri,
                            fileNames = new[] {new { fileGUID = queueItem.FileGUID, fileName=   queueItem.FileName }}
                        }
                        }
                };
                filesJSON = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            }

            //Have serialize it twice to get the proper encoding/escape, because it'll be passed as parameter to REST API call to Databricks
            filesJSON = Newtonsoft.Json.JsonConvert.SerializeObject(filesJSON);

            var tableName = string.Format(etlJob.DatabricksTableName, queueItem.SourceFileName);

            var jobParams = new string[] { "s3n", this.RootBucket,
                            fileType,
                            country.CountryName,
                            queueItem.FileGUID.ToString(),
                            tableName,
                            filesJSON,
                            CurrentIntegration.TimeZoneString,
                            "EventTime",
                            "12",
                            restatedFileGuid ?? ""
            };

            return jobParams;
        }
        #endregion

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
    }
}

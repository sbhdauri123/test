using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
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

namespace Greenhouse.Jobs.Framework
{
    [Export("GenericDeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private IEnumerable<FileFormat> _allFileFormats;

        private string JobGUID
        {
            get { return this.JED.JobGUID.ToString(); }
        }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));
            _allFileFormats = Data.Services.SetupService.GetAll<FileFormat>();
        }

        public void Execute()
        {
            Country country = Data.Services.JobService.GetById<Country>(CurrentIntegration.CountryID);
            //string sparkJobName = "GenericLogDelivery";
            var tmpSrc = new SourceFile();

            var queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(x => x.ID)
                .ToList();

            var dataSource = Data.Services.JobService.GetById<DataSource>(CurrentSource.DataSourceID);
            var sourceType = base.SourceTypes.Find(x => x.SourceTypeID == dataSource.SourceTypeID);

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

            if (etlJob == null)
            {
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
            }

            bool isFirstQueueItem = true;

            foreach (var queueItem in queueItems)
            {
                var srcFile = base.SourceFiles.FirstOrDefault(x => x.SourceFileName.Equals(queueItem.SourceFileName, StringComparison.InvariantCultureIgnoreCase));
                var tableName = string.Format(etlJob.DatabricksTableName, queueItem.SourceFileName);
                var srcFileUri = GetS3PathHelper(queueItem.EntityID, queueItem.FileDate, queueItem.FileName);
                string partitionCount = srcFile.PartitionCount?.ToString() ?? tmpSrc.PartitionCount.ToString();
                var fileFormat = _allFileFormats.FirstOrDefault(x => x.FileFormatID == srcFile.FileFormatID);

                var jobParams = new string[]
                {
                    "s3", this.RootBucket, queueItem.SourceFileName, country.CountryName, queueItem.FileGUID.ToString(),
                    tableName,
                    srcFileUri, CurrentIntegration.TimeZoneString, srcFile.PartitionColumn, partitionCount,
                    dataSource.DataSourceName, sourceType.SourceTypeName.ToLower(), $"{srcFile.FileDelimiter}",
                    srcFile.HasHeader.ToString(), fileFormat.FileFormatName
                };

                var msg = string.Format(
                    "{3},{4} - Submitting spark job for integration: {0}; source: {1}; with parameters {2}",
                    CurrentIntegration.IntegrationID, queueItem.SourceFileName, JsonConvert.SerializeObject(jobParams),
                    JobGUID, queueItem.FileGUID);
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));

                var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID,
                    queueItem, isFirstQueueItem, true, false, jobParams));
                job.Wait();

                var jsonResult = JsonConvert.SerializeObject(job.Result);
                //If job failed, then throw exception               
                if (job.Result != ResultState.SUCCESS)
                {
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
using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.DAL.Databricks;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading.Tasks;

namespace Greenhouse.Jobs.IAS.Delivery
{
    [Export("IAS-DeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private string JobGUID { get { return this.JED.JobGUID.ToString(); } }

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            CurrentIntegration = Data.Services.SetupService.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));
        }

        public void Execute()
        {
            var queueItems = Data.Services.JobService.GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(x => x.ID);
            Country country = Data.Services.JobService.GetById<Country>(CurrentIntegration.CountryID);

            var etlJobRepo = new DatabricksETLJobRepository();
            var etlJob = etlJobRepo.GetEtlJobByDataSourceID(CurrentSource.DataSourceID);

            if (etlJob == null)
            {
                throw new DatabricksETLJobNotFoundException("No DatabricksETLJob found for DataSourceID=" + CurrentSource.DataSourceID);
            }

            bool isFirstItem = true;

            foreach (var queueItem in queueItems)
            {
                //file type is case sensitive. Always starts with Capital letter.
                var cultInfo = new System.Globalization.CultureInfo("en-US", false).TextInfo;
                string fileType = cultInfo.ToTitleCase(queueItem.SourceFileName);

                //submit spark job
                var basePath = base.GetDestinationFolder();
                string[] paths = new string[] { queueItem.EntityID.ToLower(), GetDatedPartition(queueItem.FileDate), queueItem.FileName };
                var srcFileUri = $"{RemoteUri.CombineUri(basePath, paths).OriginalString.TrimStart('/')}";
                var tableName = string.Format(etlJob.DatabricksTableName, queueItem.SourceFileName);

                //job paramaters are order specific: s3Protocol, s3RootBucket, fileType, agency, country, fileGUID, rawFilePath, timeZone, partitionColumn, partitions
                //raw filepath is the FULL path to the file. i.e. s3://presto-test-123/rawtest3/IAS_floodlight685973_impression_2016041500_20160415_033815_236076527.csv.gz			
                var jobParams = new string[] { "s3", this.RootBucket, fileType, country.CountryName, queueItem.FileGUID.ToString(), tableName, srcFileUri };

                var msg = string.Format("{3} - Submitting spark job for integration: {0};source: {1}; with parameters {2}", CurrentIntegration.IntegrationID, queueItem.SourceFileName, JsonConvert.SerializeObject(jobParams), JobGUID);

                logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));
                var job = Task.Run(async () => await base.SubmitSparkJobDatabricks(etlJob.DatabricksJobID, queueItem, isFirstItem, true, false, jobParams));
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

                isFirstItem = false;
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
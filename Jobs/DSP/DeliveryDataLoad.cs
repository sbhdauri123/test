using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Framework;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Newtonsoft.Json;
using NLog;

namespace Greenhouse.Jobs.DSP.Delivery
{
    [Export("SizmekDSP-DeliveryDataLoad", typeof(IDragoJob))]
    public class DataLoadJob : BaseFrameworkJob, IDragoJob
    {
        private static Logger logger = NLog.LogManager.GetCurrentClassLogger();

        private string JobGUID
        {
            get { return this.JED.JobGUID.ToString(); }
        }


        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            CurrentIntegration = SetupServ.GetById<Integration>(GetUserSelection(Constants.US_INTEGRATION_ID));
        }

        public void Execute()
        {
            Country country = JobServ.GetById<Country>(CurrentIntegration.CountryID);
            string sparkJobName = "GenericLogDelivery";
            string partitions = "12";

            var queueItems = JobServ
                .GetQueueProcessing(CurrentIntegration.IntegrationID, this.JobLogger.JobLog.JobLogID).OrderBy(x => x.ID)
                .ToList();

            bool isFirstQueueItem = true;
            var dataSource = JobServ.GetById<DataSource>(CurrentSource.DataSourceID);
            var sourceType = base.SourceTypes.FirstOrDefault(x => x.SourceTypeID == dataSource.SourceTypeID);
            foreach (var queueItem in queueItems)
            {
                var sourceFile = base.SourceFiles.FirstOrDefault(x => x.SourceFileName.Equals(queueItem.SourceFileName, StringComparison.InvariantCultureIgnoreCase));
                var srcFileUri = GetS3PathHelper(queueItem.EntityID, queueItem.FileDate, queueItem.FileName);
                var outputPath = string.Format("{0}/processed", this.RootBucket.TrimStart('/').TrimEnd('/'));

                var jobParams = new string[]
                {
                    "s3", this.RootBucket, queueItem.SourceFileName, country.CountryName, queueItem.FileGUID.ToString(), outputPath,
                    srcFileUri, CurrentIntegration.TimeZoneString,sourceFile.PartitionColumn, partitions,
                    CurrentSource.SourceName.Replace("-Delivery", ""), sourceType.SourceTypeName.ToLower(), sourceFile.FileDelimiter
                };


                var msg = string.Format(
                    "{3} - Submitting spark job for integration: {0}; cluster: {4}; source: {1}; with parameters {2}",
                    CurrentIntegration.IntegrationID, queueItem.SourceFileName, JsonConvert.SerializeObject(jobParams),
                    JobGUID, ETLCluster.EMRClusterId);
                logger.Log(Msg.Create(LogLevel.Info, logger.Name, msg));

                var job = Task.Run(async () => await base.SubmitSparkJob(sparkJobName,
                    base.ETLCluster.EMRClusterId,
                    queueItem, isFirstQueueItem, jobParams));
                job.Wait();

                var jsonResult = JsonConvert.SerializeObject(job.Result);
                if (job.Result.Status.State != Amazon.ElasticMapReduce.StepState.COMPLETED)
                {
                    string errMessage = string.Format("ERROR->Spark job returned failure: {0}", jsonResult);
                    throw new Exception(errMessage);
                }
                else
                {
                    logger.Log(Msg.Create(LogLevel.Info, logger.Name,
                        string.Format("{3} - Completed spark job for integration: {0};source: {1}; Summary: {2}",
                            CurrentIntegration.IntegrationID, queueItem.SourceFileName, jsonResult, JobGUID)));
                }

                isFirstQueueItem = false;
            }
        }

        public void PostExecute()
        {
        }

        public void Dispose()
        {
        }
    }
}
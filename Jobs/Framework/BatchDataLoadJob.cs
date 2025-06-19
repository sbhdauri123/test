using Greenhouse.Common;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Framework
{
    [Export("AllBatchDataLoadJob", typeof(IDragoJob))]
    public class BatchDataLoadJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private readonly List<Source> _scheduledSources = new();

        private bool IsJobRunningAtSourceLevel => CurrentSource.ETLTypeID == (int)Constants.ETLProviderType.Redshift && !CurrentSource.HasIntegrationJobsChained && !CurrentSource.AggregateProcessingSettings.IntegrationProcessingRequired;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
        }

        public void Execute()
        {
            int schedCounter = 0;
            int skippedCounter = 0;
            IEnumerable<Source> allSources = SetupService.GetAll<Source>();
            IEnumerable<Integration> allIntegrations = SetupService.GetAll<Integration>();
            IEnumerable<int> queuedIntegrations = JobService.GetActiveQueueIntegrations();

            var chainedJEDs = new List<Greenhouse.Data.Model.Core.JobExecutionDetails>();

            foreach (int integrationID in queuedIntegrations)
            {
                CurrentIntegration = allIntegrations.SingleOrDefault(i => i.IntegrationID == integrationID);
                CurrentSource = allSources.Single(s => s.SourceID == CurrentIntegration.SourceID);

                if (IsJobRunningAtSourceLevel && HasSourceBeenScheduled())
                {
                    continue;
                }

                Constants.ETLProviderType etlType = (Constants.ETLProviderType)CurrentSource.ETLTypeID;

                ScheduleCalendar cal = new(CurrentServer.TimeZoneString, ScheduleCalendar.IntervalType.Minutely, null, DateTime.Now);
                Greenhouse.Data.Model.Core.JobExecutionDetails newJED = new(cal, CurrentSource, CurrentServer);
                newJED.JobProperties[Constants.US_SOURCE_ID] = CurrentIntegration.SourceID;
                newJED.JobProperties[Constants.US_INTEGRATION_ID] = CurrentIntegration.IntegrationID;

                bool ckExists = CacheStore.Exists(newJED.JobCacheKey);
                logger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Info, logger.Name, string.Format("childJobCacheKey {0} {1}", newJED.JobCacheKey, ckExists ? "EXISTS" : "DOES NOT EXIST")));
                if (!ckExists)
                {
                    if (newJED.ExecutionPath.CurrentStep != null)
                    {
                        newJED.Step = newJED.ExecutionPath.CurrentStep.Step.ParseEnum<Constants.JobStep>();

                        //ETL type is Spark - requires a cluster be obtained from the available pool to continue
                        if (etlType == Constants.ETLProviderType.Spark)
                        {
                            base.ScheduleDynamicJob(newJED);
                            logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                                string.Format(
                                    "{0} Spark Job: {1} batched and scheduled for integration: {2}",
                                    etlType.ToString(), newJED.ExecutionPath.CurrentStep.SourceJobStepName,
                                    CurrentIntegration.IntegrationID)));
                            schedCounter++;
                        }
                        //type is standard (non-Spark) ETL, just schedule the job normally
                        else
                        {
                            if (CurrentSource.HasIntegrationJobsChained)
                            {
                                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                                    $"{this.JED.JobGUID} - Adding job ({newJED.JobGUID}) to the list of chained jobs. Current Step is: {newJED.ExecutionPath.CurrentStep} "));
                                chainedJEDs.Add(newJED);
                            }
                            else
                            {
                                base.ScheduleDynamicJob(newJED);
                                schedCounter++;
                                logger.Log(Msg.Create(LogLevel.Debug, logger.Name,
                                    $"{etlType} Job: {newJED.ExecutionPath.CurrentStep.SourceJobStepName} batched and scheduled for integration: {CurrentIntegration.IntegrationID}"));
                            }
                        }
                    }
                    else
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Exepected a next step to be defined in the ExecutionPath for Source: {0} Integration: {1} but none was defined.", CurrentSource.SourceName, CurrentIntegration.IntegrationName)));
                    }
                } //endif (!ckExists)
                else
                {
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Job SKIPPED for Source: {0} Integration: {1}. Already exists in job cache.", CurrentSource.SourceName, CurrentIntegration.IntegrationName)));
                    skippedCounter++;
                }
            }
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("Batch complete - unchained - {0} child jobs scheduled and {1} child jobs were skipped", schedCounter, skippedCounter)));

            if (chainedJEDs.Count != 0)
            {
                ScheduleBatchChainedJobs(chainedJEDs);
            }
        }

        private bool HasSourceBeenScheduled()
        {
            if (_scheduledSources.Contains(CurrentSource))
            {
                return true;
            }

            _scheduledSources.Add(CurrentSource);

            return false;
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
                //nothing to dispose
            }
        }

        ~BatchDataLoadJob()
        {
            Dispose(false);
        }
    }
}

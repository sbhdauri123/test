using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
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
    [Export("GenericBatchImportJob", typeof(IDragoJob))]
    public class BatchImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private SourceJobStep _nextStep;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();

            _nextStep = JED.ExecutionPath.GotoNextStep();
            if (_nextStep == null)
            {
                throw new ArgumentNullException(string.Format("BatchImportJob always expects a next step to be defined in the ExecutionPath for it to execute."));
            }
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{1} - Next Step is: {0} ", _nextStep, this.JED.JobGUID)));
        }
        public void Execute()
        {
            System.Diagnostics.StackTrace stackTrace = new System.Diagnostics.StackTrace();
            var meth = stackTrace.GetFrame(1).GetMethod();

            int counter = 0;
            var integrations = Data.Services.SetupService.GetAll<Integration>().Where(i => i.SourceID == this.SourceId && i.IsActive);
            var source = Data.Services.SetupService.GetById<Source>(this.SourceId);
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{1} - Found: {0} Integrations", integrations.Count(), this.JED.JobGUID)));

            //Quartz.IJobDetail previousJob = null;
            var chainedJEDs = new List<Greenhouse.Data.Model.Core.JobExecutionDetails>();

            foreach (Integration integ in integrations)
            {
                CurrentIntegration = integ;
                Greenhouse.Data.Model.Core.JobExecutionDetails newJED = base.CloneJED();
                newJED.ResetExecutionGuid();
                newJED.Step = newJED.ExecutionPath.CurrentStep.Step.ParseEnum<Constants.JobStep>();
                newJED.JobProperties[Constants.US_SOURCE_ID] = CurrentIntegration.SourceID;
                newJED.JobProperties[Constants.US_INTEGRATION_ID] = CurrentIntegration.IntegrationID;

                bool ckExists = CacheStore.Exists(newJED.JobCacheKey);
                logger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Info, logger.Name, string.Format("{2} - childJobCacheKey {0} {1}", newJED.JobCacheKey, (ckExists ? "EXISTS" : "DOES NOT EXIST"), this.JED.JobGUID)));

                if (!ckExists)
                {
                    if (source.HasIntegrationJobsChained)
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, $"{this.JED.JobGUID} - Adding job ({newJED.JobGUID}) to the list of chained jobs. Current Step is: {newJED.ExecutionPath.CurrentStep} "));
                        chainedJEDs.Add(newJED);
                    }
                    else
                    {
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{1} - Current Step is: {0} ", newJED.ExecutionPath.CurrentStep, this.JED.JobGUID)));
                        base.ScheduleDynamicJob(newJED);
                        logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{2} - Job {0} batched and scheduled for integration: {1}", newJED.ExecutionPath.CurrentStep.SourceJobStepName, integ.IntegrationID, this.JED.JobGUID)));
                        counter++;
                    }
                }
                else
                {
                    logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{2} - Job SKIPPED for Source: {0} Integration: {1}. Already exists in job cache.", CurrentSource.SourceName, integ.IntegrationName, this.JED.JobGUID)));
                }
            }
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{1} - Batch complete - unchained - {0} child jobs scheduled", counter, this.JED.JobGUID)));

            if (chainedJEDs.Count != 0)
            {
                ScheduleBatchChainedJobs(chainedJEDs);
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

        ~BatchImportJob()
        {
            Dispose(false);
        }
    }
}

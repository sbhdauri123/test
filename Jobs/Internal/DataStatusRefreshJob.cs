using Greenhouse.Data.Services;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Utilities;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;

namespace Greenhouse.Jobs.Internal
{
    [Export("LogDataStatusRefreshJob", typeof(IDragoJob))]
    public class DataStatusRefreshJob : Jobs.Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private Action<string> logInfo;
        private IBackOffStrategy backoff;

        public void PreExecute()
        {
            logInfo = (msg) => logger.Log(Msg.Create(LogLevel.Info, logger.Name, PrefixJobGuid(msg)));

            backoff = new ExponentialBackOffStrategy()
            {
                Counter = 0,
                MaxRetry = 6
            };
        }

        public void PollyAction(Action call, string logName)
        {
            GetPollyPolicy<Exception>("DataStatusRefreshJob", backoff)
                .Execute((_) => { call(); },
                    new Dictionary<string, object> { { "methodName", logName } });
        }

        public void Execute()
        {
            //Log
            UpdateLogDataStatus();
        }

        private void UpdateLogDataStatus()
        {
            PollyAction(() =>
            {
                logInfo("Start of UpdateDataStatusIntegration");
                SetupService.UpdateDataStatusIntegration();
            }, "UpdateDataStatusIntegration");

            PollyAction(() =>
            {
                logInfo("Start of UpdateDataStatusSource");
                SetupService.UpdateDataStatusSource();
            }, "UpdateDataStatusSource");

            // disabling for now as not used by any Tableau board
            //logInfo("Start of UpdateEventLevelDataStatus");
            //setupServ.UpdateEventLevelDataStatus();

            logInfo("End of UpdateLogDataStatus");
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

        ~DataStatusRefreshJob()
        {
            Dispose(false);
        }
    }
}

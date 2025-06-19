using Greenhouse.Common;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using NLog;
using System;
using System.ComponentModel.Composition;

namespace Greenhouse.Jobs.Internal
{
    [Export("Hive-LogImport", typeof(IDragoJob))]
    public class HiveImportLogs : Framework.BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private int maxNbLogFileToImport;
        private int sqlCommandTimeOut;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.LOG;
            //Have to blank out sourcename for GetDestinationFolder to return right path.
            CurrentSource = new Source
            {
                SourceName = ""
            };
            if (!int.TryParse(Data.Services.SetupService.GetById<Lookup>(Constants.HIVE_MAX_NB_FILE_TO_LOAD)?.Value, out int lookupMaxNbLogFile))
                lookupMaxNbLogFile = 1000;
            maxNbLogFileToImport = lookupMaxNbLogFile;
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{0} - The maxNbLogFileToImport value is {1}.", this.JED.JobGUID.ToString(), maxNbLogFileToImport)));
            if (!int.TryParse(Data.Services.SetupService.GetById<Lookup>(Constants.HIVE_SQL_PROCESS_PARTITIONS_TIMEOUT)?.Value, out int lookupSqlCommandTimeOut))
                lookupSqlCommandTimeOut = 120;
            sqlCommandTimeOut = lookupSqlCommandTimeOut;
            logger.Log(Msg.Create(LogLevel.Debug, logger.Name, string.Format("{0} - The sqlCommandTimeOut value is {1}.", this.JED.JobGUID.ToString(), sqlCommandTimeOut)));
        }

        public void Execute()
        {
            var basePath = base.GetDestinationFolder();
            string jobGUID = this.JED.JobGUID.ToString();

            var etl = new Greenhouse.DAL.ETLProvider();
            etl.SetJobLogGUID(jobGUID);

            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{1} - Starting hive import job. S3 log path: {0}", basePath, jobGUID)));
            etl.LoadLogFile(basePath, maxNbLogFileToImport, true, sqlCommandTimeOut);
            logger.Log(Msg.Create(LogLevel.Info, logger.Name, string.Format("{0} - Completed hive import job", jobGUID)));
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

        ~HiveImportLogs()
        {
            Dispose(false);
        }
    }
}

using Greenhouse.Caching;
using Greenhouse.Logging;
using NLog;
using Quartz;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;

namespace Greenhouse.Jobs.Infrastructure
{
    public abstract class BaseQuartzJob
    {
        private static NLog.ILogger _logger;

        //To be satisfied by runtime composition
        protected IDragoJob job;
        protected IDictionary<string, object> metaData;
        protected BaseDragoJob BaseDragoJob => (BaseDragoJob)job;
        protected readonly IJobLoggerFactory JobLoggerFactory;
        protected readonly ISchedulerFactory SchedulerFactory;
        protected readonly ICacheStore Cache;

        public Greenhouse.Logging.IJobLogger JobLogger { get; set; }
        public Greenhouse.Caching.ICacheStore CacheStore { get; set; }
        public Quartz.IScheduler Scheduler { get; set; }

        protected BaseQuartzJob()
        {

        }

        protected BaseQuartzJob(IJobLoggerFactory jobLoggerFactory, ISchedulerFactory schedulerFactory,
            Greenhouse.Caching.ICacheStore cache)
        {
            JobLoggerFactory = jobLoggerFactory;
            SchedulerFactory = schedulerFactory;
            Cache = cache;
        }

        /// <summary>
        /// for versobe tracing
        /// </summary>
        /// <param name="message"></param>
        protected static void Trace(string message)
        {
            LogEventInfo lei = Msg.Create(LogLevel.Trace, _logger.Name, message);
            _logger.Log(lei);
        }

        protected static void Debug(string message)
        {
            LogEventInfo lei = Msg.Create(LogLevel.Trace, _logger.Name, message);
            _logger.Log(lei);
        }
        //The default contract key if one was not supplied in JobDetails
        protected string ContractKey = "DefaultQuery";

        /// <summary>
        /// Compose from calling assembly
        /// </summary>
        protected void ComposeFromSelf(IScheduler scheduler,
            IJobLoggerFactory jobLoggerFactory,
            Caching.ICacheStore cache,
            NLog.ILogger logger,
            Greenhouse.Data.Model.Setup.Server setupServer)
        {
            AssemblyCatalog catalog = new AssemblyCatalog(Assembly.GetExecutingAssembly());
            var container = new CompositionContainer(catalog);

            try
            {
                //Find the exported type base on contract name
                job = container.GetExportedValue<IDragoJob>(ContractKey);

                //Find the associated metadata            
                var exportedPart = container.Catalog.Parts.Where(f => f.ExportDefinitions.Any(ed => ed.ContractName == ContractKey));

                if (exportedPart != null)
                {
                    metaData = exportedPart.First().ExportDefinitions.First().Metadata;

                    Console.WriteLine("Exported Metadata:");
                    foreach (KeyValuePair<string, object> kvp in metaData)
                    {
                        Console.WriteLine("Key: {0} - Value: {1}", kvp.Key, kvp.Value);
                    }
                }
                //set the core services
                _logger = logger;
                BaseDragoJob.Logger = _logger;

                this.JobLogger = jobLoggerFactory.GetJobLogger();
                BaseDragoJob.JobLogger = this.JobLogger;

                this.CacheStore = cache;
                BaseDragoJob.CacheStore = this.CacheStore;

                this.Scheduler = scheduler;
                BaseDragoJob.Scheduler = this.Scheduler;

                BaseDragoJob.SetCurrentServer(setupServer);
            }
            catch
            {
                string msg = string.Format("A potentially fatal exception occurred during composition. The ContractKey: {0} was not found. Please ensure that the job has a valid Export statement using the format of DataSourceExportName+Step (i.e. BrightEdgeStart) and that it is marked as ISlyJob in the export. Note that jobs that use the TANGO interface must have their DataSourceExportName set to 'TANGO' in the database.", ContractKey);
                LogEventInfo lei = Msg.Create(LogLevel.Fatal, _logger.Name, msg);
                _logger.Log(lei);
            }
        }

        public static IEnumerable<IDragoJob> GetExportedJobTypes()
        {
            AssemblyCatalog catalog = new AssemblyCatalog(Assembly.GetExecutingAssembly());
            var container = new CompositionContainer(catalog);
            return container.GetExportedValues<IDragoJob>();
        }
    }
}

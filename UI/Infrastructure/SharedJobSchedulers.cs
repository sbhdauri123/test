using Quartz;
using Quartz.Impl;
using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Data.SqlClient;

namespace Greenhouse.UI
{
    public class SharedJobSchedulers
    {
        private readonly ConcurrentDictionary<string, Quartz.IScheduler> _jobSchedulers = new ConcurrentDictionary<string, Quartz.IScheduler>();

        /// <summary>
        /// Cache for job schedulers. Replacement for previously supported System.Web.HttpContext.Current.Application[tcp].
        /// </summary>
        /// <param name="schedulerInstanceName"></param>
        /// <param name="tcp"></param>
        /// <param name="stdSchedulerFactory"></param>
        /// <param name="port"></param>
        /// <param name="scsb"></param>
        /// <returns></returns>
        public async Task<IScheduler> GetAsync(string schedulerInstanceName, string tcp, string port, SqlConnectionStringBuilder scsb)
        {
            return _jobSchedulers.GetOrAdd(tcp, await CreateNewJobSchedulerAsync(schedulerInstanceName, tcp, port, scsb));
        }

        private static async Task<IScheduler> CreateNewJobSchedulerAsync(string schedulerInstanceName, string tcp, string port, SqlConnectionStringBuilder scsb)
        {
            NameValueCollection properties = new NameValueCollection();
            properties["quartz.scheduler.instanceName"] = string.Format("Quartz_{0}", schedulerInstanceName);
            // set thread pool info
            //properties["quartz.scheduler.proxy"] = "true";
            properties["quartz.threadPool.threadCount"] = "0";
            //properties["quartz.scheduler.proxy.address"] = tcp;
            properties["quartz.scheduler.instanceId"] = "AUTO";
            properties["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz";
            //properties["quartz.threadPool.threadPriority"] = Settings.Current.Quartz.ThreadPriority.ToString();//Thread priority is no longer supported, you need to remove threadPriority parameter
            properties["quartz.jobStore.misfireThreshold"] = "60000";
            properties["quartz.jobStore.type"] = "Quartz.Impl.AdoJobStore.JobStoreTX, Quartz";
            // "json" is alias for "Quartz.Simpl.JsonObjectSerializer, Quartz.Serialization.Json" 
            properties["quartz.serializer.type"] = "json";
            properties["quartz.jobStore.driverDelegateType"] = "Quartz.Impl.AdoJobStore.SqlServerDelegate, Quartz";
            properties["quartz.jobStore.tablePrefix"] = "QRTZ_";
            properties["quartz.jobStore.dataSource"] = "quartz_rc";
            properties["quartz.jobStore.lockHandler.type"] = "Quartz.Impl.AdoJobStore.UpdateLockRowSemaphore, Quartz";
            properties["quartz.jobStore.acquireTriggersWithinLock"] = "true";
            properties["quartz.dataSource.quartz_rc.provider"] = "SystemDataSqlClient";
            properties["quartz.jobStore.useProperties"] = "false";
            //remoting exporter
            properties["quartz.scheduler.exporter.type"] = "Quartz.Simpl.RemotingSchedulerExporter, Quartz";
            properties["quartz.scheduler.exporter.port"] = port;
            properties["quartz.scheduler.exporter.bindName"] = "QuartzScheduler";
            properties["quartz.scheduler.exporter.channelType"] = "tcp";
            properties["quartz.scheduler.exporter.channelName"] = "httpQuartz";
            properties["quartz.dataSource.quartz_rc.connectionString"] = scsb.ConnectionString;
            properties["quartz.jobStore.performSchemaValidation"] = "false";

            // First we must get a reference to a scheduler
            StdSchedulerFactory factory = new(properties);

            return await factory.GetScheduler();
        }
    }
}

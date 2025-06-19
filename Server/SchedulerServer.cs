using Greenhouse.Common;
using Greenhouse.Configuration;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Logging;
using Greenhouse.QuartzServer.Core;
using NLog;
using Quartz;
using System;
using System.Collections.Generic;
using System.IO;
using System.ServiceModel;
using System.Threading.Tasks;

namespace Greenhouse.Server
{
    public sealed class SchedulerServer : IQuartzServer
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private readonly Greenhouse.Data.Model.Setup.Server _server;
        private readonly string _serviceName;
        private string _version = string.Empty;
        private string _applicationEnvironment;
        private string _region;
        private string _architecture;
        private bool? _killSwitch;
        //private readonly Greenhouse.Caching.ICacheStore _cache;

        public string QuartzRemoting { get; internal set; }
        public IScheduler SchedulerInstance { get; set; }
        private readonly List<Quartz.JobKey> _quartzJobKeys = new List<Quartz.JobKey>();
        public List<Quartz.JobKey> QuartzJobKeys { get { return _quartzJobKeys; } }

        public SchedulerServer(Greenhouse.Data.Model.Setup.Server server)
        {
            _server = server;
            _serviceName = _server.ServerAlias;
        }

        /// <summary>
        /// Initializes the instance of the <see cref="QuartzServer"/> class.
        /// </summary>
        public async Task InitializeAsync()
        {
            try
            {
                _version = SchedulerServer.GetVersionNumber();
                Settings.Current.Application.AppVersion = _version;
                _applicationEnvironment = Settings.Current.Application.Environment;
                _region = Settings.Current.AWS.Region;
                _architecture = System.Environment.Is64BitOperatingSystem ? "64" : "32";

                LogMessage(string.Format("Starting {0} v-{1} ({2} bit) in Environment: {3}, AWS Region: {4}", _serviceName, _version, _architecture, _applicationEnvironment, _region));

                //Load Quartz job keys
                IReadOnlyCollection<string> groups = await SchedulerInstance.GetJobGroupNames();

                foreach (string group in groups)
                {
                    IReadOnlyCollection<JobKey> keys = await SchedulerInstance
                        .GetJobKeys(Quartz.Impl.Matchers.GroupMatcher<JobKey>.GroupContains(group));

                    foreach (JobKey key in keys)
                    {
                        _quartzJobKeys.Add(key);
                    }
                }
                //file system watcher
                Watch();
                //load the jobconfigs
                // FillTransientCacheEntries();

            }
            catch (TimeoutException timeProblem)
            {
                LogMessage(string.Format("Server initialization failed {0}", timeProblem.Message), timeProblem, NLog.LogLevel.Error);
            }
            catch (CommunicationException commProblem)
            {
                LogMessage(string.Format("Server initialization failed {0}", commProblem.Message), commProblem, NLog.LogLevel.Error);
            }
            catch (Exception e)
            {
                LogMessage(string.Format("Server initialization failed {0}", e.Message), e, NLog.LogLevel.Error);
                throw;
            }
        }

        private bool GetKillSwitch()
        {
            if (!_killSwitch.HasValue)
            {
                Lookup logLevelSDKLookup = Data.Services.SetupService.GetById<Lookup>(Constants.KILL_SWITCH);
                if (!string.IsNullOrEmpty(logLevelSDKLookup?.Value) && bool.TryParse(logLevelSDKLookup?.Value, out bool killSwitch))
                {
                    _killSwitch = killSwitch;
                }
                else
                {
                    _killSwitch = false;
                }
            }

            return _killSwitch.Value;
        }

        private static string GetVersionNumber()
        {
            string versionNumber = "unknown";
            try
            {
                versionNumber = typeof(SchedulerServer).Assembly.GetName().Version.ToString();
            }
            catch (Exception)
            {
                //ignore it
            }
            return versionNumber;
        }

        private void Watch()
        {
            string dllpath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            string path = System.IO.Path.GetDirectoryName(dllpath);

            string touchPath = Path.Combine(path, "touch.txt");
            if (!System.IO.File.Exists(touchPath))
            {
                System.IO.File.CreateText(touchPath);
            }

            LogMessage(string.Format("watch path is: {0}", path));
            FileSystemWatcher watcher = new FileSystemWatcher();
            watcher.Path = path;
            watcher.NotifyFilter = NotifyFilters.LastWrite;
            watcher.Filter = "touch.txt";

            // Add event handlers.
            watcher.Changed += new FileSystemEventHandler(LogTransientCacheEntries);
        }

        /// <summary>
        /// Starts this instance, delegates to scheduler.
        /// </summary>
        public async Task StartAsync()
        {
            try
            {
                if (GetKillSwitch())
                {
                    LogMessage("KILLSWITCH IS ON - The Scheduler's Instance won't Started", NLog.LogLevel.Error);
                    return;
                }

                //LogMessage(string.Format("Attempting to purge throttle settings for startup of Quartz Scheduler {0}", SchedulerInstance.SchedulerName));
                //Greenhouse.Jobs.Infrastructure.Throttling.IThrottle throttle = _container.Resolve<Greenhouse.Jobs.Infrastructure.Throttling.IThrottle>();
                //throttle.PurgePersistentCache();
                //LogMessage("Throttle settings purged successfully");

                LogMessage(string.Format("Attempting to start Quartz Scheduler {0}", SchedulerInstance.SchedulerName));
                await SchedulerInstance.Start();
                LogMessage(string.Format("Scheduler {0} for Server {1} started successfully", SchedulerInstance.SchedulerName, System.Environment.MachineName));
            }
            catch (Exception e)
            {
                LogMessage(string.Format("Error starting the Quartz Schedulers {0}", e.Message), e, NLog.LogLevel.Error);
                throw;
            }
        }

        /// <summary>
        /// Stops this instance, delegates to scheduler.
        /// </summary>
        public async Task StopAsync()
        {
            string schedName = (SchedulerInstance == null ? "NULL!" : SchedulerInstance.SchedulerName);

            try
            {
                //LogMessage(string.Format("Attempting to purge throttle settings for startup of Quartz Scheduler {0}", SchedulerInstance.SchedulerName));
                //Greenhouse.Jobs.Infrastructure.Throttling.IThrottle throttle = _container.Resolve<Greenhouse.Jobs.Infrastructure.Throttling.IThrottle>();
                //throttle.PurgePersistentCache();
                //LogMessage("Throttle settings purged successfully");

                LogMessage(string.Format("Attempting stop of Quartz Scheduler: {0}", schedName));
                await SchedulerInstance.Shutdown(false);
                LogMessage(string.Format("Scheduler {0} for Server {1} stopped successfully", schedName, System.Environment.MachineName));
            }
            catch (Exception e)
            {
                LogMessage(string.Format("Error stopping the Quartz Scheduler {0} Error is: {1}", schedName, e.Message), e, NLog.LogLevel.Error);
                throw;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private static void Dispose(bool disposing)
        {
            if (disposing)
            {

            }
        }

        ~SchedulerServer()
        {
            Dispose(false);
        }

        private void LogTransientCacheEntries(object sender, System.IO.FileSystemEventArgs fse)
        {
            //var keys = _cache.GetKeys();
            //foreach (string key in keys)
            //{
            //    LogMessage(string.Format("Key: {0} Value:{1}", key, _cache.Get<string>(key)));
            //}
        }

        #region Logging

        private static void LogMessage(string message)
        {
            LogMessage(message, NLog.LogLevel.Info);
        }

        private static void LogMessage(string message, NLog.LogLevel level)
        {
            message = string.Format("SchedulerServer: {0}", message);
            LogEventInfo lei = Msg.Create(level, logger.Name, message);
            logger.Log(lei);
        }

        private static void LogMessage(string message, Exception ex, NLog.LogLevel level)
        {
            message = string.Format("SchedulerServer: {0}", message);
            LogEventInfo lei = Msg.Create(level, logger.Name, message, ex);
            logger.Log(lei);
        }

        #endregion
    }
}

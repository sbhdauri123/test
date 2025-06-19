using Greenhouse.Server;
using NLog;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.WinBackgroundService
{
    public class GreenhouseWinService : BackgroundService
    {
        private readonly NLog.ILogger _logger;
        private readonly SchedulerServer _schedulerServer;

        public GreenhouseWinService(NLog.ILogger logger, SchedulerServer schedulerServer)
        {
            _logger = logger;
            _schedulerServer = schedulerServer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                LogMessage("starting...", LogLevel.Info);
                LogMessage($"SchedulerServer: QuartzJobKeys={_schedulerServer.QuartzJobKeys}", LogLevel.Info);
                LogMessage(
                    $"SchedulerInstance: SchedulerInstanceId={_schedulerServer.SchedulerInstance.SchedulerInstanceId} SchedulerName={_schedulerServer.SchedulerInstance.SchedulerName}",
                    LogLevel.Info);

                await _schedulerServer.InitializeAsync();
                await _schedulerServer.StartAsync();
            }
            catch (Exception ex)
            {
                LogMessage("stopping...", ex);
                //we tried re-throwing the exception at this point but it turns out to be even worse with the Team City build
                //because the build starts/stops the services remotely it will just hang the process. This way at least it completes quickly
                // throw e;

                // Terminates this process and returns an exit code to the operating system.
                // This is required to avoid the 'BackgroundServiceExceptionBehavior', which
                // performs one of two scenarios:
                // 1. When set to "Ignore": will do nothing at all, errors cause zombie services.
                // 2. When set to "StopHost": will cleanly stop the host, and log errors.
                //
                // In order for the Windows Service Management system to leverage configured
                // recovery options, we need to terminate the process with a non-zero exit code.
                Environment.Exit(1);
            }
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
            LogMessage("stopping...", LogLevel.Info);
            await _schedulerServer.StopAsync();
        }

        private void LogMessage(string message, NLog.LogLevel level)
        {
            message = String.Format("GreenhouseWinService: {0}", message);
            LogEventInfo lei = Greenhouse.Logging.Msg.Create(level, _logger.Name, message);
            _logger.Log(lei);
        }

        private void LogMessage(string message, Exception ex)
        {
            message = String.Format("GreenhouseWinService: {0}", message);
            LogEventInfo lei = Greenhouse.Logging.Msg.Create(NLog.LogLevel.Error, _logger.Name, message, ex);
            _logger.Log(lei);
        }
    }
}
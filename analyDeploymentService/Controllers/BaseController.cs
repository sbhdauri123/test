using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.DeploymentService.Controllers
{
    public class BaseController : Controller
    {
        protected NLog.ILogger JobLogger { get; }

        protected readonly string JobGuid;

        public BaseController(NLog.ILogger logger)
        {
            this.JobLogger = logger;
            this.JobGuid = System.Guid.NewGuid().ToString();
        }

        protected void LogInfoMsg(string msg)
        {
            JobLogger.Info($"{JobGuid}: {msg}");
        }

        protected void LogDebugMsg(string msg)
        {
            JobLogger.Debug($"{JobGuid}:  {msg}");
        }

        protected void LogWarnMsg(string msg)
        {
            JobLogger.Warn($"{JobGuid}:  {msg}");
        }

        protected void LogErrorMsg(Exception ex, string msg)
        {
            JobLogger.Error(ex, $"{JobGuid}: {msg}");
        }
    }
}

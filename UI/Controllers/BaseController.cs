using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Services;
using Microsoft.AspNetCore.Mvc;
using LogLevel = NLog.LogLevel;

namespace Greenhouse.UI.Controllers
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

        protected void LogInfoMessage(string msg)
        {
            JobLogger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Info, JobLogger.Name, msg));
        }

        protected void LogDebugMesage(string msg)
        {
            JobLogger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Debug, JobLogger.Name, msg));
        }

        protected void LogWarnMessage(string msg)
        {
            JobLogger.Log(Greenhouse.Logging.Msg.Create(LogLevel.Warn, JobLogger.Name, msg));
        }

        protected string? LogException(Exception ex, string msg)
        {
            NLog.LogEventInfo lei = Greenhouse.Logging.Msg.Create(LogLevel.Error, JobLogger.Name, msg, ex);
            JobLogger.Error(ex, msg);
            return lei.Properties[Logging.Msg.GUID] as string;
        }

        protected string LogException(System.Exception ex)
        {
            return this.LogException(ex, string.Empty);
        }

        protected void SaveAuditLog(AuditLog auditLog)
        {
            auditLog.ModifiedBy = User.Identity?.Name;
            SetupService.Add(auditLog);
        }
    }
}
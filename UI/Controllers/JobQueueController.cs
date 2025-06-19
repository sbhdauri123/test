using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.UI.Controllers
{
    public class JobQueueController : BaseController
    {
        public JobQueueController(NLog.ILogger logger) : base(logger)
        {

        }

        //// GET: JobQueue
        public ActionResult Index()
        {
            return View();
        }

        [HttpGet]
        public JsonResult GetAllQueueLogs(DateTime startDate)
        {
            var logs = Data.Services.JobService.GetAllQueueLogs(startDate);
            var orderedQueueLogs = logs.OrderBy(x => x.EntityID).ThenBy(y => y.StatusSortOrder).ThenByDescending(y => y.LastUpdated);

            return new JsonResult(orderedQueueLogs);
        }
    }
}

using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.UI.Controllers
{
    public class RegexController : BaseController
    {
        public RegexController(NLog.ILogger logger) : base(logger)
        {
        }

        // GET: Regex
        public ActionResult Index()
        {
            return View();
        }
    }
}
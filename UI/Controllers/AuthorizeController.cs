using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.UI.Controllers
{
    public class AuthorizeController : BaseController
    {
        public AuthorizeController(NLog.ILogger logger) : base(logger)
        {
        }

        // GET: Authorize
        public ActionResult Index()
        {
            return View();
        }
    }
}
//using Kendo.Mvc.Examples.Models.Themes;
using Greenhouse.UI.Services.Setup;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;

namespace Greenhouse.UI.Controllers
{
    [AllowAnonymous]
    [Route("api/[controller]")]
    [ApiController]
    public class ServerController : BaseController
    {

        public ServerController(NLog.ILogger logger, IHubContext<ServerHub> notificationContext) : base(logger)
        {

        }
    }
}

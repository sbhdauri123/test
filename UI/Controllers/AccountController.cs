using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.UI.Controllers
{
    [AllowAnonymous]
    [ViewComponentAttribute]
    public class _Header : ViewComponent
    {
        public IViewComponentResult Invoke()
        {
            Greenhouse.UI.Models.Menu greenhouseMenu = new Greenhouse.UI.Models.Menu();

            if (User != null)
            {
                //TODO: to be removed when we move away from UserAuthorization table
                if (User.IsInRole("admin"))
                {
                    greenhouseMenu.Admin = true;
                    greenhouseMenu.MenuLinks = AccountController.GetMenuLinks(greenhouseMenu.Admin);
                    greenhouseMenu.Name = User.Identity.Name;
                    greenhouseMenu.Email = User.Identity.Name;
                }
            }

            return View("_Header", greenhouseMenu);
        }
    }

    [Authorize]
    public class AccountController : BaseController
    {
        public AccountController(NLog.ILogger logger) : base(logger)
        {
        }

        public static List<Greenhouse.UI.Models.MenuLinks> GetMenuLinks(bool Admin)
        {
            List<Greenhouse.UI.Models.MenuLinks> menuLinks;

            menuLinks = new List<Greenhouse.UI.Models.MenuLinks>()
            {
                 new Greenhouse.UI.Models.MenuLinks {
                     text = "Infrastructure Management",
                     items = new  List<Greenhouse.UI.Models.MenuItem>()
                     {
                        new Greenhouse.UI.Models.MenuItem{ text = "Server Setup", url = "/Setup/Servers"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Instance Setup", url = "/Setup/Instances"},
                     }
                 },
                new Greenhouse.UI.Models.MenuLinks
                {
                    text = "Data Source Management",
                    items = new List<Greenhouse.UI.Models.MenuItem>()
                    {
                        new Greenhouse.UI.Models.MenuItem{ text = "Data Source Setup", url = "/Setup/DataSources"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Source Setup", url = "/Setup/Sources"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Source File Setup", url = "/Setup/SourceFiles"},
                    }
                },
                 new Greenhouse.UI.Models.MenuLinks {
                     text = "Integration Management",
                     items = new  List<Greenhouse.UI.Models.MenuItem>()
                     {
                        new Greenhouse.UI.Models.MenuItem{ text = "Credential Setup", url = "/Setup/Credentials"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Integration Setup", url = "/Setup/Integrations"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Job Scheduler", url = "/JobScheduler/Index"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Job Logs", url = "/JobLog/Index"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Job Queue", url = "/JobQueue/Index"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Advertiser Permissions", url = "/Setup/AdvertiserMappings" },
                        new Greenhouse.UI.Models.MenuItem{ text = "Redshift User Admin", url = "/Setup/ManageUsers"},
                        new Greenhouse.UI.Models.MenuItem{ text = "Advertiser Admin", url = "/Setup/AdvertiserAdmin" },
                        new Greenhouse.UI.Models.MenuItem{ text = "API Entity Setup", url = "/Setup/APIEntities" },
                        new Greenhouse.UI.Models.MenuItem{ text = "Custom Fields Setup", url = "/Setup/CustomFields" }
                     }
                 },
                new Greenhouse.UI.Models.MenuLinks {
                    text = "Utilities",
                    items = new List<Greenhouse.UI.Models.MenuItem>()
                    {
                        new Greenhouse.UI.Models.MenuItem{ text = "Regex Builder", url = "/Utilities/RegexBuilder" },
                        new Greenhouse.UI.Models.MenuItem{ text = "Authorize", url = "/Utilities/Authorization" },
                        new Greenhouse.UI.Models.MenuItem{ text = "Lookup Setup", url = "/Setup/Lookup" }
                    }
                },
                 new Greenhouse.UI.Models.MenuLinks {
                     text = "Ad Tag Modifier",
                     items = new List<Greenhouse.UI.Models.MenuItem>()
                     {
                         new Greenhouse.UI.Models.MenuItem{ text = "Advertisers", url = "/AdTag/Advertisers" },
                         new Greenhouse.UI.Models.MenuItem{ text = "Job Runs", url = "/AdTag/JobRun" }
                     }
                 }
            };

            return menuLinks;
        }
    }
}
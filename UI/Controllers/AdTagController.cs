using Greenhouse.Data.Model.AdTag;
using Greenhouse.Data.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace Greenhouse.UI.Controllers
{
    public class AdTagController : BaseController
    {
        public AdTagController(NLog.ILogger logger) : base(logger)
        {
        }

        #region Advertiser

        public ActionResult Advertisers()
        {
            return View("Advertisers");
        }

        #endregion

        #region JobRun

        public ActionResult JobRun()
        {
            return View("JobRun");
        }

        #endregion

        #region PlacementRequests

        public ActionResult PlacementRequests()
        {
            return View("PlacementRequests");
        }

        #endregion

        #region Setup General

        [HttpGet]
        public ActionResult GetAPIAdServerRequests()
        {
            AdTagAdvertiserRepository repo = new AdTagAdvertiserRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAPIAdServerRequestById(string apiAdServerRequestId)
        {
            ServerRepository repo = new ServerRepository();
            var data = repo.GetById(apiAdServerRequestId);
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetGreenhouseDataSources()
        {
            AdTagAdVendorRepository repo = new AdTagAdVendorRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAllAdvertisers()
        {
            AdTagAdvertiserRepository repo = new AdTagAdvertiserRepository();
            IEnumerable<Advertiser> advertisers = repo.GetAll<Advertiser>("GetAllAdvertisers");
            List<Advertiser> advertiserList = advertisers.ToList<Advertiser>();

            Advertiser all = new Advertiser();
            all.AdvertiserID = 0;
            all.AdvertiserName = "All";

            advertiserList.Insert(0, all);

            return Json(advertiserList);
        }
        #endregion
    }
}

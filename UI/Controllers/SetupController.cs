using Greenhouse.Common;
using Greenhouse.Configuration;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Data.Repositories;
using Greenhouse.Data.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Newtonsoft.Json;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.UI.Controllers
{
    public class SetupController : BaseController
    {
        public SetupController(NLog.ILogger logger) : base(logger)
        {
        }

        #region DataSource
        public ActionResult DataSources()
        {
            return View("DataSources");
        }
        public ActionResult GetDataSources()
        {
            DataSourceRepository repo = new DataSourceRepository();
            var data = repo.GetAll();
            return Ok(data);
        }

        #endregion

        #region Servers
        public ActionResult Servers()
        {
            List<SelectListItem> tz = TimeZoneInfo.GetSystemTimeZones().OrderBy(x => x.DisplayName).Select(x => new SelectListItem() { Value = x.ToSerializedString(), Text = x.DisplayName }).ToList();
            ViewBag.TimeZones = tz;
            return View("Servers");
        }

        public ActionResult GetServerTypes()
        {
            ServerTypeRepository repo = new ServerTypeRepository();
            var data = repo.GetAll();
            return Json(data);
        }
        #endregion

        public ActionResult GetClusters()
        {
            ClusterRepository repo = new ClusterRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        #region Instances
        public ActionResult Instances()
        {
            return View("Instances");
        }

        public ActionResult GetCountries()
        {
            CountryRepository repo = new CountryRepository();
            var data = repo.GetAll();
            return Json(data);
        }
        #endregion

        #region Credentials
        public ActionResult Credentials()
        {
            return View("Credentials");
        }
        #endregion

        #region AdvertiserMappings

        public ActionResult AdvertiserMappings()
        {
            var datasourceRepo = new DataSourceRepository();
            var datasources = datasourceRepo.GetAll().ToList();
            var instanceRepo = new InstanceRepository();
            var instance = instanceRepo.GetAll().ToList();

            ViewBag.DataSource = datasources;
            ViewBag.Instance = instance;

            return View("AdvertiserMappings");
        }

        public ActionResult GetAllAdvertisers(string datasourceId, string instanceId, bool isAggregate)
        {
            var adRepo = new AdvertiserMappingRepository();

            var ads = adRepo.GetAllAdvertisersByID(datasourceId, instanceId, isAggregate);

            if (ads.Any())
            {
                var result = Json(ads);
                //result.MaxJsonLength = int.MaxValue;
                return result;
            }
            else return Json(false);
        }

        //public ActionResult GetAvailableAdvertisers(AdvertiserMapping adModel, string instanceId)
        public ActionResult GetAvailableAdvertisers(string datasourceId, string instanceId)
        {
            var adRepo = new AdvertiserMappingRepository();

            var ads = adRepo.GetAvailableAdvertisersByID(datasourceId, instanceId);

            if (ads.Any())
            {
                var result = Json(ads);
                //result.MaxJsonLength = int.MaxValue;
                return result;
            }
            else return Json(false);
        }

        public ActionResult GetMappedAdvertisers(string datasourceId, string instanceID)
        {
            var adRepo = new AdvertiserMappingRepository();

            var ads = adRepo.GetMappedAdvertisersByID(datasourceId, instanceID);

            if (ads.Any()) return Json(ads);
            else return Json(false);
        }

        //[HttpPost]
        //public ActionResult SaveMappedAdvertiser(AdvertiserMapping adModel)
        //{
        //    var adRepo = new AdvertiserMappingRepository();

        //    var instanceRepo = new InstanceRepository();
        //    var instance = instanceRepo.GetById(adModel.InstanceID);

        //    adModel.MasterAgencyID = instance.MasterAgencyID;
        //    adModel.IsActive = true;

        //    adRepo.Update(adModel);

        //    var ads = adRepo.GetAdvertisersByIDs(adModel.DataSourceID.ToString(), adModel.InstanceID.ToString());

        //    if (ads.Any()) return Json(ads, JsonRequestBehavior.AllowGet);
        //    else return Json(false, JsonRequestBehavior.AllowGet);
        //}

        [HttpPost]
        public ActionResult SaveMapped(string advertiserMappingIDs, int instanceID)
        {
            //Log the modification attempt
            base.SaveAuditLog(new AuditLog
            {
                AppComponent = "Advertiser Mapping",
                Action = "Map",
                AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    AdvertiserMappingIDs = advertiserMappingIDs,
                    InstanceID = instanceID
                })
            });

            var connString = Settings.Current.Greenhouse.GreenhouseDimDbConnectionString;

            using (var connection = new SqlConnection(connString))
            {
                connection.Open();
                var cmd = connection.CreateCommand();
                cmd.Connection = connection;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "UpdateAdvertiserMapping";
                cmd.Parameters.AddWithValue("@AdvertiserMappingIDs", advertiserMappingIDs);
                cmd.Parameters.AddWithValue("@InstanceID", instanceID);
                cmd.ExecuteScalar();
            }

            return Json(true);
        }

        [HttpPost]
        public ActionResult DeleteMappedAdvertiser(int instanceID, int advertiserMappingID)
        {
            //Log the modification attempt
            base.SaveAuditLog(new AuditLog
            {
                AppComponent = "Advertiser Unmapping",
                Action = "Unmap",
                AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    AdvertiserMappingID = advertiserMappingID,
                    InstanceID = instanceID
                })
            });

            var connString = Settings.Current.Greenhouse.GreenhouseDimDbConnectionString;
            using (var connection = new SqlConnection(connString))
            {
                connection.Open();
                var cmd = connection.CreateCommand();
                cmd.Connection = connection;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "DeleteInstanceAdvertiserMapping";
                cmd.Parameters.AddWithValue("@InstanceID", instanceID);
                cmd.Parameters.AddWithValue("@AdvertiserMappingID", advertiserMappingID);
                cmd.ExecuteScalar();
            }

            return Json(true);
        }

        #endregion

        #region AdvertiserAdmin

        public ActionResult AdvertiserAdmin()
        {
            return View("AdvertiserAdmin");
        }

        //public ActionResult GetSecurityAdvertisers(SecurityAdvertiserMapping adModel)
        //{
        //    var adRepo = new SecurityAdvertiserMappingRepository();

        //    var ads = adRepo.GetAdvertisersByID(adModel.DataSourceID.ToString());

        //    if (ads.Any()) return Json(ads, JsonRequestBehavior.AllowGet);
        //    else return Json(false, JsonRequestBehavior.AllowGet);
        //}

        #endregion

        #region Sources

        [HttpGet]
        public ActionResult Sources()
        {
            return View("Sources");
        }

        #endregion

        #region SourceFiles

        [HttpGet]
        public ActionResult SourceFiles()
        {
            return View("SourceFiles");
        }

        #endregion

        #region Security

        [HttpGet]
        public ActionResult UserAccess()
        {
            return View("UserAccess");
        }

        #endregion

        #region SchedulerConfiguration

        [HttpGet]
        public ActionResult SchedulerConfigurations()
        {
            return View("SchedulerConfigurations");
        }

        public ActionResult getMatchTypeResults(string Mask, string FileNames)
        {
            Greenhouse.Utilities.RegexCodec rc = new Utilities.RegexCodec(Mask);

            string result1 = rc.FileNameRegex.ToString() + "^^^";

            string delimeter = "\n";
            if (FileNames.Contains("\r\n"))
                delimeter = "\r\n";

            string[] lines = FileNames.Split(new string[] { delimeter }, StringSplitOptions.RemoveEmptyEntries);

            string result2 = "";
            foreach (string line in lines)
            {
                result2 += string.Format("{0} : {1}", line, rc.FileNameRegex.IsMatch(line)) + ",";
            }
            result2 = result2.Substring(0, result2.Length - 1);

            return Json(result1 + result2);
        }

        #endregion

        #region Integration

        [HttpGet]
        public ActionResult Integrations()
        {
            return View("Integrations");
        }

        [HttpGet]
        public ActionResult GetActiveIntegrations()
        {
            var integrationRepo = new IntegrationRepository();

            // hiding "child" integrations (where parent ID is not null)
            var data = integrationRepo.GetAll().Where(i => i.ParentIntegrationID == null).Select(p => new
            {
                ActiveIntegrationId = p.IntegrationID,
                ActiveIntegrationName = p.IntegrationName,
                SourceID = p.SourceID
            }).OrderBy(d => d.ActiveIntegrationName).ToList();

            return Json(data);
        }

        [HttpGet]
        public ActionResult GetIntegrations()
        {
            IntegrationRepository repo = new IntegrationRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpPost]
        public ActionResult DeleteIntegrationUponValidation(int integrationID)
        {
            var item = SetupService.GetById<Integration>(integrationID);

            if (item != null)
            {
                Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();

                var entity = typeof(Integration);
                auditLog.AppComponent = entity.Name;
                auditLog.Action = Constants.AuditLogAction.Delete.ToString();
                auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    ModifiedValue = item
                });

                auditLog.CreatedDate = DateTime.Now;
                SaveAuditLog(auditLog);
            }

            var connString = Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
            object result;
            using (var connection = new SqlConnection(connString))
            {
                connection.Open();
                var cmd = connection.CreateCommand();
                cmd.Connection = connection;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "DeleteIntegrationUponValidation";
                cmd.Parameters.AddWithValue("@IntegrationID", integrationID);
                result = cmd.ExecuteScalar();
            }

            return Json(result);
        }
        #endregion
        #region API Entities
        [HttpGet]
        public ActionResult APIEntities()
        {
            return View("APIEntities");
        }
        [HttpPost]
        public ActionResult DeleteAPIEntityUponValidation(int aPIEntityID)
        {
            var item = SetupService.GetById<APIEntity>(aPIEntityID);

            if (item != null)
            {
                Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();

                var entity = typeof(APIEntity);
                auditLog.AppComponent = entity.Name;
                auditLog.Action = Constants.AuditLogAction.Delete.ToString();
                auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
                {
                    ModifiedValue = item
                });

                auditLog.CreatedDate = DateTime.Now;
                SetupService.Delete<APIEntity>(aPIEntityID);
                SaveAuditLog(auditLog);

                return Json(new { IsAPIEntityDeleted = true });
            }
            return Json(new { IsAPIEntityDeleted = false });
        }

        #endregion

        #region Custom Fields
        [HttpGet]
        public ActionResult CustomFields()
        {
            int maxSavedColumnsSA360 = int.Parse(SetupService.GetById<Lookup>(Constants.SEARCHADS360_MAX_SAVED_COLUMNS).Value);

            int maxSavedColumnsSkai = int.Parse(SetupService.GetById<Lookup>(Constants.SKAI_MAX_SAVED_COLUMNS).Value);

            ViewBag.MaxActiveFields = JsonConvert.SerializeObject(new { sa360 = maxSavedColumnsSA360, skai = maxSavedColumnsSkai });
            return View("CustomFields");
        }
        #endregion

        #region Setup General

        [HttpGet]
        public ActionResult GetGreenhouseSources()
        {
            SourceRepository repo = new SourceRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetGreenhouseDataSources()
        {
            DataSourceRepository repo = new DataSourceRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAPIEntitySources()
        {
            SourceRepository repo = new SourceRepository();
            var allSources = repo.GetAll();

            var value = (SetupService.GetById<Lookup>(Constants.MANAGE_API_OTHER_SOURCES).Value);
            var sourceIDs = value.Split(',').Select(x => int.Parse(x));

            var supportedEtlTypes = new[] { Constants.UI_ETLTYPEID_REDSHIFT, Constants.UI_ETLTYPEID_DATABRICKS };

            var data = allSources.Where(x => supportedEtlTypes.Contains(x.ETLTypeID) || sourceIDs.Contains(x.SourceID)).OrderBy(x => x.SourceName);

            return Json(data);
        }

        [HttpGet]
        public ActionResult GetMasterClients()
        {
            MasterClientRepository repo = new MasterClientRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetMasterAgencies()
        {
            MasterAgencyRepository repo = new MasterAgencyRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAgencyInstances()
        {
            InstanceRepository repo = new InstanceRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetServers()
        {
            ServerRepository repo = new ServerRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetJobCategories()
        {
            JobCategoryRepository repo = new JobCategoryRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetJobTypes()
        {
            JobTypeRepository repo = new JobTypeRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAllCrendentials()
        {
            CredentialRepository repo = new CredentialRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAllCountries()
        {
            CountryRepository repo = new CountryRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public ActionResult GetAllTimeZones()
        {
            TimeZoneRepository repo = new TimeZoneRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        [HttpGet]
        public JsonResult GetExecutionTypes()
        {
            var ExecutionTypes = ((IEnumerable<Constants.ExecutionType>)Enum.GetValues(typeof(Constants.ExecutionType)))
                                     .Select(x =>
                                         new
                                         {
                                             ExecutionTypeID = (int)x,
                                             ExecutionTypeName = x.ToString()
                                         });

            return Json(ExecutionTypes);
        }

        [HttpGet]
        public ActionResult GetETLTypes()
        {
            ETLTypeRepository repo = new ETLTypeRepository();
            var data = repo.GetAll();
            return Json(data);
        }

        #endregion

        #region Redshift User Management
        [HttpGet]
        public ActionResult ManageUsers()
        {
            return View("ManageUsers");
        }

        [HttpGet]
        public JsonResult GetUsers()
        {
            Data.Repositories.UserMappingRepository repo = new Data.Repositories.UserMappingRepository();
            var result = repo.GetAllUsersMapping();
            return Json(result);
        }

        [HttpGet]
        public JsonResult GetRedshiftUsersUnmapped()
        {
            var redshiftRepository = new RedshiftRepository();

            var users = RedshiftRepository.GetUserIDs();

            Data.Repositories.UserMappingRepository repo = new Data.Repositories.UserMappingRepository();
            var userMappings = repo.GetAllUsersMapping().Select(u => u.UserID).ToList();
            var response = users.Where(u => !userMappings.Contains(u)).Select(u => new { UserName = u });

            return Json(response);
        }

        [HttpGet]
        public JsonResult GetUserMapping(string userID, bool isAdvertiser)
        {
            Data.Repositories.UserMappingRepository repo = new Data.Repositories.UserMappingRepository();
            var result = repo.GetUserMapping(userID)?.Where(x => x.IsAdvertiser == isAdvertiser);
            return Json(result ?? new List<UserMapping>());
        }

        [HttpGet]
        public JsonResult GetAdvertisers()
        {
            AdvertiserMappingRepository advertiserRepo = new AdvertiserMappingRepository();

            var result = advertiserRepo.GetAllAggregateAdvertiserMapping()
                                ?.Select(x => new { x.AdvertiserMappingID, x.DataSourceID, x.DataSourceName, x.AdvertiserNameDisplay });

            return Json(result);
        }

        [HttpGet]
        public JsonResult GetInstances()
        {
            InstanceRepository instanceRepo = new InstanceRepository();
            var result = instanceRepo.GetItems(new { isPM = false }).Select(x => new { x.InstanceID, x.InstanceName });
            return Json(result);
        }

        #endregion

        #region Lookup

        [HttpGet]
        public ActionResult Lookup()
        {
            return View("Lookup");
        }

        #endregion
    }
}

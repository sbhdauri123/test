using Dapper;
using Greenhouse.Common;
using Greenhouse.Data.Model.AdTag;
using Greenhouse.Data.Repositories;
using Newtonsoft.Json;

namespace Greenhouse.UI.Services.AdTag
{
    public class AdvertiserHub : BaseHub<APIAdServerRequest>
    {
        public AdvertiserHub(Greenhouse.Data.Services.AdTagService service) : base(service)
        {
        }

        private AdTagAdvertiserRepository adTagAdvertiserRepository;

        public IEnumerable<APIAdServerRequest> ReadAll(string guid)
        {
            adTagAdvertiserRepository = new AdTagAdvertiserRepository();

            string proc = "GetAPIAdServerRequestsByAdVendorID";
            var apiAdServerRequests = adTagAdvertiserRepository.GetAll<APIAdServerRequest>(proc, new KeyValuePair<string, string>("DataSourceID", guid));
            return apiAdServerRequests;
        }

        public APIAdServerRequest ProcessAdvertiser(APIAdServerRequest modifiedItem)
        {
            APIAdServerRequest origItem = null;

            adTagAdvertiserRepository = new AdTagAdvertiserRepository();

            string modifiedItemJSON = string.Empty;

            Data.Model.Setup.AuditLog auditLog = new Data.Model.Setup.AuditLog();
            auditLog.AppComponent = Constants.AuditLogComponent.AdTag_Advertiser.ToString();

            if (modifiedItem.APIAdServerRequestID != 0)
            {
                origItem = adTagAdvertiserRepository.GetAll<APIAdServerRequest>("GetAPIAdServerRequestByID", new KeyValuePair<string, string>("APIAdServerRequestID", modifiedItem.APIAdServerRequestID.ToString())).FirstOrDefault();
                auditLog.Action = Constants.AuditLogAction.Update.ToString();
            }
            else
            {
                modifiedItem.IsAPIAdServer = true;
                auditLog.Action = Constants.AuditLogAction.Create.ToString();
            }

            DynamicParameters parameters = new Dapper.DynamicParameters();
            parameters.Add("@APIAdServerRequestID", modifiedItem.APIAdServerRequestID);
            parameters.Add("@AdvertiserName", modifiedItem.AdvertiserName);
            parameters.Add("@UserName", modifiedItem.UserName);
            parameters.Add("@ProfileID", modifiedItem.ProfileID);
            parameters.Add("@TagVersion", modifiedItem.TagVersion);
            parameters.Add("@PASDetail", modifiedItem.PASDetail);
            parameters.Add("@PairDelimiter", modifiedItem.PairDelimiter);
            parameters.Add("@KeyValueDelimiter", modifiedItem.KeyValueDelimiter);
            parameters.Add("@IsActive", modifiedItem.IsActive);
            parameters.Add("@IsOutputToReport", modifiedItem.IsOutputToReport);
            parameters.Add("@WriteToReportStatus", modifiedItem.WriteToReportStatus);
            parameters.Add("@IsAPIAdServer", modifiedItem.IsAPIAdServer);
            parameters.Add("@AccountID", modifiedItem.AccountID);
            parameters.Add("@AdvertiserID", modifiedItem.AdvertiserID);
            parameters.Add("@InitialPlacementID", modifiedItem.InitialPlacementID);
            parameters.Add("@AdVendorID", modifiedItem.AdVendorID);

            int apiAdServerRequestID = adTagAdvertiserRepository.GetAll<APIAdServerRequest>("ProcessAPIAdServerRequest", parameters).FirstOrDefault().APIAdServerRequestID;

            modifiedItem.APIAdServerRequestID = apiAdServerRequestID;

            auditLog.AdditionalDetails = JsonConvert.SerializeObject(new
            {
                OriginalValue = origItem,
                ModifiedValue = modifiedItem
            });
            auditLog.CreatedDate = DateTime.Now;
            base.SaveAuditLog(auditLog);

            return modifiedItem;
        }
    }
}
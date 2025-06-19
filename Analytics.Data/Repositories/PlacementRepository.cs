using Greenhouse.Data.Model.AdTag.APIAdServer;
using NLog;
using System;

namespace Greenhouse.Data.Repositories
{
    public class PlacementRepository : AdTagBaseRepository<Placement>
    {
        private static readonly Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public string GetAdditionalKeyValues(string profileId, string advertiserId, string placementName, string placementId)
        {
            string AdditionalKeyValues = string.Empty;

            try
            {
                var parameters = new Dapper.DynamicParameters();
                parameters.Add("AdVendorMasterAccountID", profileId, System.Data.DbType.String, System.Data.ParameterDirection.Input);
                parameters.Add("AdVendorSubAccountID", advertiserId, System.Data.DbType.String, System.Data.ParameterDirection.Input);
                parameters.Add("PlacementName", placementName, System.Data.DbType.String, System.Data.ParameterDirection.Input);
                parameters.Add("PlacementID", Convert.ToInt64(placementId), System.Data.DbType.Int64, System.Data.ParameterDirection.Input);
                parameters.Add("uValue", dbType: System.Data.DbType.String, direction: System.Data.ParameterDirection.Output, size: 4000);

                QueryStoredProc("GenerateuValue", parameters);
                AdditionalKeyValues = parameters.Get<string>("uValue");
            }
            catch (Exception ex)
            {
                logger.Log(NLog.LogLevel.Error, string.Format("ERROR: {0}\nStack Trace:\n{1}", ex.Message, ex.StackTrace));
                throw;
            }
            return AdditionalKeyValues;
        }
    }
}

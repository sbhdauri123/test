using Dapper;
using Greenhouse.Data.Model.Setup;
using System;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class UserMappingRepository : DimensionBaseRepository<Greenhouse.Data.Model.Setup.UserMapping>
    {
        public IEnumerable<Model.Setup.AdvertiserMapping> GetAvailableAdvertisersByID(string datasourceId, string instanceId)
        {
            if (string.IsNullOrEmpty(instanceId)) instanceId = "0";

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@DataSourceID", Convert.ToInt32(datasourceId));
            parameters.Add("@InstanceID", Convert.ToInt32(instanceId));

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.AdvertiserMapping>("GetAvailableAdvertisersByIDs", param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<UserMapping> GetAllUsersMapping()
        {
            var results = base.QueryStoredProc("GetAllUsersMapping");
            return results;
        }

        public IEnumerable<UserMapping> GetUserMapping(string userID)
        {
            if (string.IsNullOrEmpty(userID))
                return null;

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@UserID", userID);
            var results = base.QueryStoredProc("GetUserMapping", parameters);
            return results;
        }
    }
}
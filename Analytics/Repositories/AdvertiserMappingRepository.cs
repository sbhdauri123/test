using Dapper;
using System;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class AdvertiserMappingRepository : DimensionBaseRepository<Greenhouse.Data.Model.Setup.AdvertiserMapping>
    {
        public IEnumerable<Model.Setup.AvailableAdvertiser> GetAllAdvertisersByID(string datasourceId, string instanceId, bool isAggregate)
        {
            if (string.IsNullOrEmpty(instanceId))
                instanceId = "0";

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@DataSourceID", Convert.ToInt32(datasourceId));
            parameters.Add("@InstanceID", Convert.ToInt32(instanceId));
            parameters.Add("@IsAggregate", isAggregate);

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.AvailableAdvertiser>("GetAllAdvertisersByIDs", param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<Model.Setup.AvailableAdvertiser> GetAvailableAdvertisersByID(string datasourceId, string instanceId)
        {
            if (string.IsNullOrEmpty(instanceId))
                instanceId = "0";

            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@DataSourceID", Convert.ToInt32(datasourceId));
            parameters.Add("@InstanceID", Convert.ToInt32(instanceId));

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.AvailableAdvertiser>("GetAvailableAdvertisersByIDs", param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<Model.Setup.MappedAdvertiser> GetMappedAdvertisersByID(string datasourceId, string instanceId)
        {
            var parameters = new Dapper.DynamicParameters();
            parameters.Add("@DataSourceID", Convert.ToInt32(datasourceId));
            parameters.Add("@InstanceID", Convert.ToInt32(instanceId));

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.MappedAdvertiser>("GetMappedAdvertisersByID", param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<Model.Setup.AdvertiserMapping> GetAllAggregateAdvertiserMapping()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.AdvertiserMapping>("GetAllAggregateAdvertiserMappings", param: null, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<Model.Setup.AdvertiserMapping> GetAll<TEntity>(string procName, params KeyValuePair<string, string>[] kvp)
        {
            var parameters = new Dapper.DynamicParameters();
            for (int i = 0; i < kvp.Length; i++)
            {
                parameters.Add(@kvp[i].Key, kvp[i].Value);
            }

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.AdvertiserMapping>(procName, param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public new void Update(Model.Setup.AdvertiserMapping AdvertiserMapping)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Update<Model.Setup.AdvertiserMapping>(AdvertiserMapping);
            }
        }
    }
}
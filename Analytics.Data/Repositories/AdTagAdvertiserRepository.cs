using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public class AdTagAdvertiserRepository : AdTagBaseRepository<Model.AdTag.APIAdServerRequest>
    {
        public IEnumerable<TEntity> GetAll<TEntity>(string procName, params KeyValuePair<string, string>[] kvp)
        {
            var parameters = new Dapper.DynamicParameters();
            if (kvp != null)
            {
                for (int i = 0; i < kvp.Length; i++)
                {
                    parameters.Add(@kvp[i].Key, kvp[i].Value);
                }
            }

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<TEntity>(procName, param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public IEnumerable<TEntity> GetAll<TEntity>(string procName, DynamicParameters parameters = null)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<TEntity>(procName, param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public Model.AdTag.Advertiser GetAdvertiserDetails(long advertiserID)
        {
            DynamicParameters parameters = new Dapper.DynamicParameters();
            parameters.Add("@AdvertiserID", advertiserID);

            Model.AdTag.Advertiser advertiser = GetAll<Model.AdTag.Advertiser>("GetAdvertiserDetailsByAdvertiserID", parameters).FirstOrDefault();

            return advertiser;
        }
    }
}

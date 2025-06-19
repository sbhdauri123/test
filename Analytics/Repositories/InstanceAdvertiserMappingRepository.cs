using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public class InstanceAdvertiserMappingRepository : DimensionBaseRepository<Greenhouse.Data.Model.Setup.HivePartition>
    {
        public int UpdateAdvertiserMapping(int instanceID, int advertiserMappingID)
        {
            int result = 0;
            using (IDbConnection connection = OpenConnection())
            {
                result = connection.Execute("UPDATE [dbo].[InstanceAdvertiserMapping] set IsAddedToMetastore=1, LastUpdated=GETDATE() WHERE AdvertiserMappingID = @advertiserMappingID AND InstanceID = @instanceID ", new { advertiserMappingID, instanceID });
            }
            return result;
        }

        /// <summary>
        /// </summary>
        /// <param name="keys">Expected Tuple keys are AdvertiserMappingID and InstanceID</param>
        /// <returns></returns>
        public int UpdateAdvertiserMappings(IEnumerable<System.Tuple<string, string>> keys)
        {
            int result = 0;
            using (IDbConnection connection = OpenConnection())
            {
                result = connection.Execute("UPDATE [dbo].[InstanceAdvertiserMapping] set IsAddedToMetastore=1, LastUpdated=GETDATE() WHERE AdvertiserMappingID = @advertiserMappingID AND InstanceID = @instanceID", keys.Select(x => new { advertiserMappingID = int.Parse(x.Item1), instanceID = int.Parse(x.Item2) }));
            }
            return result;
        }
    }
}

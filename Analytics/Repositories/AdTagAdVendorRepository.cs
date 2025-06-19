using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class AdTagAdVendorRepository : AdTagBaseRepository<Greenhouse.Data.Model.AdTag.AdVendor>
    {
        new public IEnumerable<Model.AdTag.AdVendor> GetAll()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.AdTag.AdVendor>(string.Format("select * from [{0}] order by AdVendorName", typeof(Model.AdTag.AdVendor).Name));
            }
        }
    }
}
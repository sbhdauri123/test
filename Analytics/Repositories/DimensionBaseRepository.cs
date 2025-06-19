using Greenhouse.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class DimensionBaseRepository<TEntity> : AbstractBaseRepository<TEntity>
    {
        protected override IDbConnection OpenConnection()
        {
            var connStr = Settings.Current.Greenhouse.GreenhouseDimDbConnectionString;
            SqlConnection connection = new SqlConnection(connStr);
            connection.Open();
            return connection;
        }
    }
}
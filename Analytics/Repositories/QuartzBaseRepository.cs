using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class QuartzBaseRepository<TEntity> : AbstractBaseRepository<TEntity>
    {
        protected override IDbConnection OpenConnection()
        {
            var connStr = Greenhouse.Configuration.Settings.Current.Quartz.ConnectionString;
            SqlConnection connection = new SqlConnection(connStr);
            connection.Open();
            return connection;
        }
    }
}
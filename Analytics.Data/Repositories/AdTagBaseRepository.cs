using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class AdTagBaseRepository<TEntity> : AbstractBaseRepository<TEntity>
    {
        public static string AdTagConnectionString => Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString.Replace("Configuration", "AdTagGenerator");

        protected override IDbConnection OpenConnection()
        {
            SqlConnection connection = new SqlConnection(AdTagConnectionString);
            connection.Open();
            return connection;
        }
    }
}
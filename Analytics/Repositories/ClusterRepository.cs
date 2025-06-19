using Dapper;
using Greenhouse.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class ClusterRepository : BaseRepository<Greenhouse.Data.Model.Setup.Cluster>
    {
        public ClusterRepository() : base()
        {
        }

        protected override IDbConnection OpenConnection()
        {
            string connectionString = Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;

            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        public IEnumerable<Model.Setup.Cluster> GetClustersByEnv(string env)
        {
            string connStr = Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
            using (IDbConnection connection = OpenConnection(connStr))
            {
                return connection.Query<Model.Setup.Cluster>("SELECT * FROM CLUSTER");
            }
        }
    }
}

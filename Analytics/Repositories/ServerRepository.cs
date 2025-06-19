using Dapper;
using Greenhouse.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class ServerRepository : BaseRepository<Greenhouse.Data.Model.Setup.Server>
    {
        public ServerRepository() : base()
        {
        }

        protected override IDbConnection OpenConnection()
        {
            string connectionString = Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;

            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        /// <summary>
        /// For use only by Deployment project for now
        /// </summary>
        /// <param name="env"></param>
        /// <returns></returns>
        public IEnumerable<Model.Setup.Server> GetServersByEnv(string env)
        {
            string connStr = Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
            using (IDbConnection connection = OpenConnection(connStr))
            {
                return connection.Query<Model.Setup.Server>("SELECT * FROM SERVER");
            }
        }

        public IEnumerable<Model.Setup.Server> GetServers(int executionTypeID)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.Server>("SELECT * FROM SERVER WHERE ISNULL(ExecutionTypeID, 0) = @ExecutionTypeID ", new { ExecutionTypeID = executionTypeID });
            }
        }
    }
}

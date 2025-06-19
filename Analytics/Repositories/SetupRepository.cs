using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class SetupRepository<T> : BaseRepository<T>
    {
        private readonly IConfiguration _greenhouseConfiguration;
        public SetupRepository(IConfiguration config) : base()
        {
            _greenhouseConfiguration = config;
        }

        protected override IDbConnection OpenConnection()
        {
            string connectionString = _greenhouseConfiguration.GetConnectionString("DEV");

            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }
    }
}

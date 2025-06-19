using Dapper;
using Greenhouse.Configuration;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace Greenhouse.Data.Repositories
{
    public class ServerTypeRepository : BaseRepository<Greenhouse.Data.Model.Setup.ServerType>
    {
        public ServerTypeRepository() : base()
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
        /// This is a stub method to show custom Repo call. Should be deleted once the Cluster UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public IEnumerable<Model.Setup.ServerType> ServerTypeRepoCustomMethod(int id)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.ServerType>(string.Format("select * from ServerType where ServerTypeID = @id "), new { id = id });
            }
        }
    }
}

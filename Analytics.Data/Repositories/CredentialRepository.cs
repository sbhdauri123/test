using Dapper;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class CredentialRepository : BaseRepository<Greenhouse.Data.Model.Setup.Credential>
    {
        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Cluster UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public IEnumerable<Model.Setup.Credential> CredentialRepoCustomMethod(int id)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.Credential>(string.Format("select * from Credential where CredentialID = @id "), new { id = id });
            }
        }
    }
}

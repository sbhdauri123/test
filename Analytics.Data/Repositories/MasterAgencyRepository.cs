using Dapper;
using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class MasterAgencyRepository : BaseRepository<Model.Setup.MasterAgency>
    {
        public MasterAgencyRepository() : base()
        {
        }

        public new IEnumerable<MasterAgency> GetAll()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var v = connection.Query<MasterAgency>(string.Format("select * from [{0}]", typeof(MasterAgency).Name));
                return v;
            }
        }
        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Source UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        //public IEnumerable<Model.Setup.Source> SourceRepoCustomMethod(int id)
        //{
        //    using (IDbConnection connection = OpenConnection())
        //    {
        //        return connection.Query<Model.Setup.Source>(string.Format("select * from Source where SourceID = @id "), new { id = id });
        //    }
        //}
    }
}

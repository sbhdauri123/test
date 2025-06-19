namespace Greenhouse.Data.Repositories
{
    public class DataSourceRepository : BaseRepository<Greenhouse.Data.Model.Setup.DataSource>
    {

        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Cluster UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        //public IEnumerable<Model.Setup.Cluster> DataSourceRepoCustomMethod(int id)
        //{
        //    using (IDbConnection connection = OpenConnection())
        //    {
        //        return connection.Query<Model.Setup.Cluster>(string.Format("select * from DataSource where DataSourceID = @id "), new { id = id });
        //    }
        //}
    }
}
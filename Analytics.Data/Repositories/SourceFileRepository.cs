namespace Greenhouse.Data.Repositories
{
    public class SourceFileRepository : BaseRepository<Model.Setup.SourceFile>
    {

        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Source UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        //public IEnumerable<Model.Setup.SourceFile> GetAllByID(int SourceID)
        //{
        //    using (IDbConnection connection = OpenConnection())
        //    {
        //        return connection.Query<Model.Setup.SourceFile>(string.Format("select * from SourceFile where SourceID = @id "), new { id = SourceID });
        //    }
        //}
    }
}

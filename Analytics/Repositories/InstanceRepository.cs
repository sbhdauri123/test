using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public class InstanceRepository : DimensionBaseRepository<Model.Setup.Instance>
    {
        /// <summary>
        /// Retrieve Instance object from DB based on its Id
        /// </summary>
        /// <param name="id">Id of the object to retrieve</param>
        /// <returns>Instance of the Instance object</returns>
        public new Model.Setup.Instance GetById(object id)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.Instance>($"select * from Instance where InstanceID={id}").FirstOrDefault();
            }
        }

        /// <summary>
        /// This is a stub method to show custom Repo call. Should be deleted once the Source UI is finalized.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public new IEnumerable<Model.Setup.Instance> GetAll()
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<Model.Setup.Instance>("select * from Instance");
            }
        }

        public new int? Add(Model.Setup.Instance instance)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Insert<int, Model.Setup.Instance>(instance);
            }
        }

        public new void Delete(Model.Setup.Instance instance)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Delete<Model.Setup.Instance>(instance);
            }
        }

        public new void Update(Model.Setup.Instance instance)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Update<Model.Setup.Instance>(instance);
            }
        }
    }
}

using Greenhouse.Data.Model.Setup;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public class DatabricksETLJobRepository : BaseRepository<DatabricksETLJob>
    {
        /// <summary>
        /// For use only by Deployment project for now
        /// </summary>
        /// <param name="env"></param>
        /// <returns></returns>
        public DatabricksETLJob GetEtlJobByDataSourceID(int dataSourceID)
        {
            return this.GetItems(new { DataSourceID = dataSourceID }).FirstOrDefault();
        }

        /// <summary>
        /// Aggregate sources are processed at source level
        /// </summary>
        /// <param name="sourceID"></param>
        /// <returns></returns>
        public DatabricksETLJob GetEtlJobBySourceID(int sourceID)
        {
            return this.GetItems(new { SourceID = sourceID }).FirstOrDefault();
        }
    }
}

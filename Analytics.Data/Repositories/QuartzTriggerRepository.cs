using Dapper;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class QuartzTriggerRepository : QuartzBaseRepository<Model.Core.QuartzTrigger>
    {
        public new void Update(Model.Core.QuartzTrigger QuartzTrigger)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Update<Model.Core.QuartzTrigger>(QuartzTrigger);
            }
        }
    }
}
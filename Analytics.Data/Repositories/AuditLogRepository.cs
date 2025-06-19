using Dapper;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class AuditLogRepository : BaseRepository<Model.Setup.AuditLog>
    {
        public new int? Add(Model.Setup.AuditLog auditLog)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Insert<int, Model.Setup.AuditLog>(auditLog);
            }
        }
    }
}
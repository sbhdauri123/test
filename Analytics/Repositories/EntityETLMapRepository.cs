using Dapper;
using Greenhouse.Data.Model.Aggregate;
using System.Collections.Generic;
using System.Data;

namespace Greenhouse.Data.Repositories
{
    public class EntityETLMapRepository : BaseRepository<EntityETLMap>
    {
        public IEnumerable<ETLScript> GetEntityEtlScript(int sourceId)
        {
            using (var connection = OpenConnection())
            {
                var result = connection.Query<ETLScript>("GetMappedEtlScript", new { SourceID = sourceId }, commandType: CommandType.StoredProcedure);
                return result;
            }
        }

        public IEnumerable<MappedReportsResponse<T>> GetEntityAPIReports<T>(int sourceId)
        {
            IEnumerable<MappedReportsResponse<T>> mappedReports;
            var fieldRepo = new BaseRepository<APIReportField>();

            using (var connection = OpenConnection())
            {
                mappedReports = connection.Query<MappedReportsResponse<T>>("GetMappedAPIReports", new { SourceID = sourceId }, commandType: CommandType.StoredProcedure);
            }

            foreach (var rpt in mappedReports)
            {
                rpt.ReportFields = fieldRepo.GetItems(new { rpt.APIReportID, IsActive = true });
            }

            return mappedReports;
        }
    }
}

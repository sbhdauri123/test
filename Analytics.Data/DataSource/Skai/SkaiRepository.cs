using Greenhouse.Data.DataSource.Skai.CustomMetrics;
using Greenhouse.Data.Repositories;
using System.Collections.Generic;

namespace Greenhouse.Data.DataSource.Skai
{
    public class SkaiRepository : RedshiftRepository
    {
        public static IEnumerable<SkaiProfile> GetAllSkaiProfiles(string serverId)
        {
            var query = $@"
            SELECT profile_id AS ProfileID,
                   server_id AS ServerID,
                   profile_name AS ProfileName,
                   profile_status AS ProfileStatus
            FROM dti_skai.profile
            WHERE server_id = {serverId};
            ";
            return ExecuteRedshiftDataReader<SkaiProfile>(query);
        }

        public static IEnumerable<SkaiSavedColumn> GetSkaiColumns()
        {
            var query = $@"SELECT server_id as ServerID
                    , profile_id as ProfileID
                    , group_name as GroupName
                    , available_column_id as ID
                    , available_column_name as ColumnName
                    , CASE is_active WHEN true Then 'True' ELSE 'False' END as IsActive
                    , entity as Entity
                from dti_skai.available_column ac
                where ac.is_deleted = 0;";
            return ExecuteRedshiftDataReader<SkaiSavedColumn>(query);
        }
    }
}
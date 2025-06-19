using Dapper;
using Greenhouse.Common;
using Greenhouse.Data.Model.Aggregate;
using Greenhouse.Data.Model.APIEntity;
using Greenhouse.Data.Model.Setup;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Greenhouse.Data.Repositories;

public class APIEntityRepository : BaseRepository<Model.Aggregate.APIEntity>
{
    public static string GetAPIEntityTimeZone(string entityId, IEnumerable<APIEntity> apiEntities, Integration integration)
    {
        var data = apiEntities.FirstOrDefault(e => e.APIEntityCode.Equals(entityId, StringComparison.InvariantCultureIgnoreCase));
        if (!string.IsNullOrEmpty(data?.TimeZone))
        {
            return data.TimeZone;
        }

        return integration?.TimeZoneString ?? Constants.AGGREGATE_DEFAULT_TIMEZONE;
    }

    public IEnumerable<string> GetDuplicateAPIEntityCodes(string sourceID)
    {
        using (IDbConnection connection = OpenConnection())
        {
            return connection.Query<string>($"SELECT APIEntityCode FROM APIEntity WHERE SourceID = {sourceID} GROUP BY APIEntityCode HAVING count(*) > 1 order by APIEntityCode;");
        }
    }

    public void DeactivateInactiveAPIEntities(IEnumerable<InactiveEntity> inactiveEntities, string sourceID)
    {
        using (IDbConnection connection = OpenConnection())
        {
            var query = $"UPDATE APIEntity SET IsActive = 0, LastUpdated = GETDATE() WHERE APIEntityCode IN @entityCodes AND SourceID = {sourceID};";
            connection.Execute(query, new { entityCodes = inactiveEntities.Select(x => x.Entity_ID) });
        }
    }
}

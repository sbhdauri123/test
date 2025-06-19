using Greenhouse.Data.Repositories;
using System;
using System.Collections.Generic;
using System.Data.Odbc;

namespace Greenhouse.Data.DataSource.DataCertification
{
    public class DataCertificationRepository : RedshiftRepository
    {
        public static IEnumerable<BackfillQueue> GetPendingRequests(Guid guid)
        {
            List<OdbcParameter> parameters = new()
            {
                new System.Data.Odbc.OdbcParameter() { ParameterName = "fileguid", Value = guid }
            };

            var query = @"
set query_group to '@fileguid';
CALL dti_dataquality.get_new_backfill_requests ();
SELECT stg.backfill_key AS BackfillKey,
       stg.source_id AS SourceID,
       stg.integration_id AS IntegrationID,
       stg.entity_id AS EntityID,
       stg.file_date AS FileDate,
        CASE stg.bf_scheduled WHEN true Then 'True' ELSE 'False' END AS BackfillScheduled,
       stg.fileguid AS Fileguid,
       stg.id AS QueueID,
       stg.createddate AS CreatedDate,
       stg.lastupdated AS LastUpdated,
       api.entitypriorityorder AS EntityPriorityOrder
FROM dti_dataquality.data_certification_backfill_staging stg
  LEFT JOIN dbo.apientity api
    ON stg.source_id = api.sourceid
    AND stg.entity_id = api.apientitycode
WHERE ISNULL(stg.fileguid,'') = '';
reset query_group;";
            var sql = PrepareCommandText(query, parameters);
            return ExecuteRedshiftDataReader<BackfillQueue>(sql);
        }

        public static void ProcessNewQueues(IEnumerable<OdbcParameter> parameters)
        {
            var redshiftProcessSql = @"
set query_group to '@fileguid';
CALL dti_dataquality.load_backfill_stage_table('@stagefilepath', '@iamrole', '@region');
reset query_group;
";
            var sql = PrepareCommandText(redshiftProcessSql, parameters);
            var result = ExecuteRedshiftCommand(sql);
        }

        public static void LoadSourceSettings(IEnumerable<OdbcParameter> parameters)
        {
            var redshiftProcessSql = @"
set query_group to '@fileguid';
CALL dti_dataquality.load_backfill_source_settings('@stagefilepath', '@iamrole', '@region');
reset query_group;
";
            var sql = PrepareCommandText(redshiftProcessSql, parameters);
            var result = ExecuteRedshiftCommand(sql);
        }
    }
}

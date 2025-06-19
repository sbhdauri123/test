using Greenhouse.Data.Model.Setup;
using System.Collections.Generic;

namespace Greenhouse.Data.Repositories
{
    public class CustomFieldRepository : RedshiftRepository
    {
        public static IEnumerable<SA360CustomField> GetSA360SavedColumns()
        {
            var query = @"SELECT nvl(functional_column_name, sc.saved_column_name) as SavedColumnName
, sc.agency_id as AgencyID
, ag.agency as AgencyName
, sc.advertiser_id as AdvertiserID
, ad.advertiser as AdvertiserName
, CASE sc.is_active WHEN true Then 'True' ELSE 'False' END as IsActive
, sc.createddate as CreatedDate
, sc.lastupdated as LastUpdated
FROM dti_sa360.saved_column sc
LEFT JOIN dti_sa360.agency ag ON ag.agencyid = sc.agency_id
LEFT JOIN dti_sa360.advertiser ad ON ad.advertiserid = sc.advertiser_id
ORDER BY sc.createddate asc;
";
            return ExecuteRedshiftDataReader<SA360CustomField>(query);
        }

        public static IEnumerable<CustomField> GetAllSavedColumns()
        {
            var query = @"SELECT schematablename as TableSource
	   , retrieved_column_name as RetrievedColumnName
       , functional_column_name as FunctionalColumnName
       , parent_id as ParentID
       , parent_name as ParentName
       , child_id as ChildID
       , child_name as ChildName
       , CASE is_active WHEN true Then 'True' ELSE 'False' END as IsActive
       , createddate as CreatedDate
       , lastupdated as LastUpdated
        , entity as Entity
        FROM dti.custom_field order by RetrievedColumnName ASC;";
            return ExecuteRedshiftDataReader<CustomField>(query);
        }

        public static bool Update(CustomField item)
        {
            //if the value is an empty string or just spaces, set it to null
            if (item.FunctionalColumnName != null && item.FunctionalColumnName.Trim().Length == 0)
            {
                item.FunctionalColumnName = null;
            }
            else
            {
                item.FunctionalColumnName = item.FunctionalColumnName?.Replace("'", "''");
            }

            var functionalValue = string.IsNullOrEmpty(item.FunctionalColumnName)
                ? "null"
                : $"'{item.FunctionalColumnName}'";

            if (item.TableSource.Contains("sa360"))
            {
                var query = $@"CALL dti_sa360.update_saved_column(
                {item.IsActive} ,
                '{item.RetrievedColumnName.Replace("'", "''")}',
                {functionalValue},
                {item.ParentID},
                {item.ChildID})";
                ExecuteRedshiftCommand(query);
            }
            else if (item.TableSource.Contains("skai"))
            {
                var query = $@"CALL dti_skai.update_available_column(
                {item.IsActive} ,
                '{item.RetrievedColumnName.Replace("'", "''")}',
                {functionalValue},
                {item.ParentID},
                {item.ChildID},
                {item.Entity})";
                ExecuteRedshiftCommand(query);
            }

            return true;
        }
    }
}
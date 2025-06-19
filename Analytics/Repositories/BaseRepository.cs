using Greenhouse.Data.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Greenhouse.Data.Repositories
{
    public class BaseRepository<TEntity> : AbstractBaseRepository<TEntity>
    {
        protected IDbConnection OpenConnection(string connectionString = null)
        {
            if (String.IsNullOrEmpty(connectionString))
            {
                connectionString = Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
            }
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        protected override IDbConnection OpenConnection()
        {
            string connectionString = Greenhouse.Configuration.Settings.Current.Greenhouse.GreenhouseConfigDbConnectionString;
            SqlConnection connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        public void BulkInsert<T>(IEnumerable<T> inserts, string tableName) where T : BasePOCO
        {
            if (inserts == null || !inserts.Any())
            {
                return;
            }

            using (SqlConnection connection = (SqlConnection)OpenConnection())
            {
                inserts = inserts.Select(i =>
                {
                    i.CreatedDate = DateTime.Now;
                    i.LastUpdated = DateTime.Now;
                    return i;
                });

                var dtExt = new Utilities.DataTables.ObjectShredder<T>();
                var dataTable = dtExt.Shred(inserts, null, null);

                using (var sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.FireTriggers, null))
                {
                    var cols = dataTable.Columns.Cast<DataColumn>().ToList();
                    //sqlBulkCopy bug: Object properties order need to match db columns order. Set columnMappings to fix this issue
                    foreach (DataColumn header in cols)
                    { sbc.ColumnMappings.Add(header.ColumnName, header.ColumnName); }

                    sbc.BulkCopyTimeout = 1000;
                    sbc.BatchSize = 1000;

                    sbc.DestinationTableName = tableName;
                    sbc.WriteToServer(dataTable);
                }

                dataTable.Clear();
                dataTable.Dispose();
            }
        }
    }
}
using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace Greenhouse.Data.Repositories
{
    public abstract class AbstractBaseRepository<TEntity> : IRepository<TEntity>
    {
        protected abstract IDbConnection OpenConnection();

        protected static void SetIdentity<T>(IDbConnection connection, Action<T> setId)
        {
            dynamic identity = connection.Query("SELECT @@IDENTITY AS Id").Single();
            T newId = (T)identity.Id;
            setId(newId);
        }

        public IEnumerable<TEntity> GetAll()
        {
            using (IDbConnection connection = OpenConnection())
            {
                var v = connection.Query<TEntity>(string.Format("select * from [{0}]", typeof(TEntity).Name));
                return v;
            }
        }

        public TEntity GetById(object id)
        {
            var entity = typeof(TEntity);

            var props = GetPropertyInfo();

            // If composite key, we'll use "or" condition. Else, straight up condition
            var filter = string.Join(" or ", props.Select(x => string.Format("{0} = @id ", x.Name)));

            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<TEntity>(string.Format("select * from [{0}] where {1} ", entity.Name, filter), new { id = id }).FirstOrDefault();
            }
        }

        public IEnumerable<TEntity> GetItems(object whereConditions)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.GetList<TEntity>(whereConditions: whereConditions);
            }
        }

        public IEnumerable<TEntity> GetItems(string sqlToExecute)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<TEntity>(sqlToExecute);
            }
        }

        public int? Add(TEntity entity)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Insert<int, TEntity>(entity);
            }
        }

        public int? AddAsync(TEntity entity)
        {
            using IDbConnection connection = OpenConnection();
            return connection.InsertAsync<int, TEntity>(entity).GetAwaiter().GetResult();
        }

        public void Delete(object id)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Delete<TEntity>(id);
            }
        }

        public void Delete(TEntity entityToDelete)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Delete<TEntity>(entityToDelete);
            }
        }

        public void Update(TEntity entityToUpdate)
        {
            using (IDbConnection connection = OpenConnection())
            {
                connection.Update<TEntity>(entityToUpdate);
            }
        }

        public void UpdateAsync(TEntity entityToUpdate)
        {
            using IDbConnection connection = OpenConnection();
            connection.UpdateAsync<TEntity>(entityToUpdate).GetAwaiter().GetResult();
        }

        public IEnumerable<TEntity> QueryStoredProc(string storedProcName, DynamicParameters parameters = null)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return connection.Query<TEntity>(storedProcName, param: parameters, commandType: CommandType.StoredProcedure);
            }
        }

        public int ExecuteStoredProc(string storedProcName, DynamicParameters parameters = null, IDbTransaction trans = null)
        {
            using (IDbConnection connection = OpenConnection())
            {
                return ExecuteStoredProc(connection, storedProcName, parameters, trans);
            }
        }

        public int ExecuteStoredProc(IDbConnection connection, string storedProcName, DynamicParameters parameters = null, IDbTransaction trans = null, int? timeOut = null)
        {
            return connection.Execute(storedProcName, param: parameters, commandType: CommandType.StoredProcedure, commandTimeout: timeOut, transaction: trans);
        }

        public IEnumerable<System.Reflection.PropertyInfo> GetPropertyInfo()
        {
            var entity = typeof(TEntity);
            // using reflection to get key column(s) from type
            var props = entity.GetProperties()
                .Where(x => x.CustomAttributes.Any(a =>
                    a.AttributeType == typeof(Dapper.KeyAttribute)));

            return props;
        }

        public void BulkInsert<T>(IEnumerable<T> inserts, SqlConnection connection, string destTableName = null, SqlTransaction trans = null)
        {
            if (inserts == null || !inserts.Any())
            {
                return;
            }

            if (string.IsNullOrEmpty(destTableName))
            {
                destTableName = typeof(T).GetCustomAttribute<TableAttribute>().Name;
            }

            var dtExt = new Utilities.DataTables.ObjectShredder<T>();
            var dataTable = dtExt.Shred(inserts, null, null);

            using (var sbc = new SqlBulkCopy(connection, SqlBulkCopyOptions.FireTriggers, trans))
            {
                var cols = dataTable.Columns.Cast<DataColumn>().ToList();
                //sqlBulkCopy bug: Object properties order need to match db columns order. Set columnMappings to fix this issue
                foreach (DataColumn header in cols)
                { sbc.ColumnMappings.Add(header.ColumnName, header.ColumnName); }

                sbc.BulkCopyTimeout = 1000;
                sbc.BatchSize = 1000;

                sbc.DestinationTableName = destTableName;
                sbc.WriteToServer(dataTable);
            }

            dataTable.Clear();
            dataTable.Dispose();
        }
    }
}

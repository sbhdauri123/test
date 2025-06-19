using Greenhouse.Data.Repositories;
using System.Collections.Generic;

namespace Greenhouse.Data.Services
{
    public class AdTagService : IBaseService
    {
        public AdTagService()
        {
        }

        public IEnumerable<TEntity> GetAll<TEntity>()
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetAll();
        }

        public static IEnumerable<TEntity> GetAll<TEntity>(string procName, params KeyValuePair<string, string>[] kvp)
        {
            var parameters = new Dapper.DynamicParameters();
            for (int i = 0; i < kvp.Length; i++)
            {
                parameters.Add(@kvp[i].Key, kvp[i].Value);
            }

            var baseRepository = new BaseRepository<TEntity>();

            return baseRepository.QueryStoredProc(procName, parameters);
        }

        public TEntity GetById<TEntity>(object id)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetById(id);
        }

        public static IEnumerable<TEntity> GetItems<TEntity>(object whereClause)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetItems(whereClause);
        }

        public int? Add<TEntity>(TEntity entityToAdd)
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.Add(entityToAdd);
        }

        public void Update<TEntity>(TEntity entityToUpdate)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Update(entityToUpdate);
        }

        public void Delete<TEntity>(object id)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Delete(id);
        }

        public void Delete<TEntity>(TEntity entityToDelete)
        {
            var baseRepository = new BaseRepository<TEntity>();
            baseRepository.Delete(entityToDelete);
        }

        public static IEnumerable<System.Reflection.PropertyInfo> GetPropertyInfo<TEntity>()
        {
            var baseRepository = new BaseRepository<TEntity>();
            return baseRepository.GetPropertyInfo();
        }
    }
}

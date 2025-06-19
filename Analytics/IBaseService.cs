using System.Collections.Generic;

namespace Greenhouse.Data
{
    public interface IBaseService
    {
        IEnumerable<TEntity> GetAll<TEntity>();

        TEntity GetById<TEntity>(object id);

        int? Add<TEntity>(TEntity entity);

        void Delete<TEntity>(object id);

        void Delete<TEntity>(TEntity entityToDelete);

        void Update<TEntity>(TEntity entityToUpdate);
    }
}

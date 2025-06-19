using System.Collections.Generic;

namespace Greenhouse.Data
{
    public interface IRepository<TEntity>
    {
        TEntity GetById(object id);

        int? Add(TEntity entity);

        void Delete(object id);

        void Delete(TEntity entityToDelete);

        void Update(TEntity entityToUpdate);

        IEnumerable<TEntity> GetAll();
    }
}

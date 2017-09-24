using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TableStorageRepository.Core
{
    /// <summary>
    /// This interface should be externalised into a common lib.  That way, any of your other IRepository Implementations can use this
    /// common lib, and give you an easy easy way to swap different implementations, such as Mongo, Cosmos or even SQL.
    /// </summary>
    public interface IRepository<TEntity> where TEntity: class
    {
		void Dispose();
		Task<int> CommitAsync();
		Task InsertAsync(TEntity entity);
		Task UpdateAsync(TEntity entity);
		Task DeleteAsync(TEntity entity);
		Task DeleteAsync(object id);
		Task<List<TEntity>> GetListAsync();
		Task<TEntity> FindAsync(object id);
		Task<TEntity> GetAsync(string filter);
    }
}

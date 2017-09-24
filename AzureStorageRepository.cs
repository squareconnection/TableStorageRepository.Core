using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using TableStorageRepository.Core.SquareConnection.AzureTableConnector;

namespace TableStorageRepository.Core
{
	public class AzureStorageRepository<T> : IRepository<T> where T : class, new()
	{
		CloudStorageAccount storageAccount;
		CloudTableClient tableClient;
		CloudTable table;
		TableContinuationToken continuationToken;
		string tableName = "";

		private string partitionKey;
		private List<T> mappedList;

		public AzureStorageRepository(string TableName, string PartitionKey, string StorageConnectionString)
		{
			Initialise(TableName, PartitionKey, StorageConnectionString);
		}

		internal async void Initialise(string TableName, string PartitionKey, string StorageConnectionString)
		{
			this.tableName = TableName;
			this.partitionKey = PartitionKey;
			this.continuationToken = new TableContinuationToken();

			storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
			tableClient = storageAccount.CreateCloudTableClient();
			table = tableClient.GetTableReference(tableName);
			await table.CreateIfNotExistsAsync();
			mappedList = new List<T>();
		}

		public async Task InsertAsync(T entity)
		{
			TableOperation insertOperation = TableOperation.Insert(CreateDTO(entity));
			await table.ExecuteAsync(insertOperation);
		}

		/// <summary>
		/// Returns a list of the first 1000 objects in the database.  Advise using GetPaged to limit returned results
		/// </summary>
		/// <returns></returns>
		public async Task<List<T>> GetListAsync()
		{
			// Construct the query operation for all customer entities where PartitionKey="Smith".
			var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
			List<T> result = new List<T>();

			var results = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
			return await StripDTOListAsync(results.ToList());
		}

		public async Task<T> GetAsync(string filter)
		{
			var parsedResults = QueryParser.GetFilter(filter);
			var query = new TableQuery().Where(
				TableQuery.CombineFilters(
					TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
					TableOperators.And,
					TableQuery.GenerateFilterCondition(parsedResults.PropertyName, parsedResults.Operation, parsedResults.GivenValue)
					)
				);

			var results = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
			var result = await StripDTOAsync(results.FirstOrDefault());


			return result;
		}

		public async Task DeleteAsync(T entity)
		{
			if (entity != null)
			{
				TableOperation deleteOperation = TableOperation.Delete(CreateDTO(entity));

				await table.ExecuteAsync(deleteOperation);
			}
			else
			{
				throw new ArgumentNullException();
			}
		}

		public async Task DeleteAsync(object id)
		{
			var entity = await FindAsync(id);
			if (entity != null)
			{
				TableOperation deleteOperation = TableOperation.Delete(CreateDTO(entity));

				await table.ExecuteAsync(deleteOperation);
			}
			else
			{
				throw new IndexOutOfRangeException();
			}

		}




		public void Dispose()
		{
			throw new NotImplementedException();
		}


        public async Task<T> FindAsync(object id)
        {
            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, id.ToString()));
            var dto = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
            T mapped = await StripDTOAsync(dto.FirstOrDefault());

            return mapped;
        }

		public async Task UpdateAsync(T entity)
		{
			TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(CreateDTO(entity));
			await table.ExecuteAsync(insertOrReplaceOperation);
		}

		internal int Commit()
		{
			return 1; //for now we commit immediately.
					  //TODO: Implement EF type caching/comitt for batch processes.
		}

		public async Task<int> CommitAsync()
		{
			return await Task.Run(() => Commit());
		}



		/// <summary>
		/// This returns all the properties of our Model - this is needed so that we can dynamically construct JSON models for use by KnockoutJS etc.
		/// </summary>
		/// <returns></returns>
		internal List<string> Meta()
		{
			List<string> schema = new List<string>();
			var entity = typeof(T);

			foreach (var p in entity.GetProperties())
			{
				schema.Add(p.Name);
			}

			return schema;
		}

		public async Task<List<string>> MetaAsync()
		{
			return await Task.Run(() => Meta());
		}

		#region object mapping
		dynamic CreateDTO(object a)
		{
			TableEntityDTO dto = new TableEntityDTO();
			object rowKey = null;

			Type t1 = a.GetType();
			Type t2 = dto.GetType();




			//now set all the entity properties
			foreach (System.Reflection.PropertyInfo p in t1.GetProperties())
			{
				Type t = p.PropertyType;

				bool isNested = t.IsNested;
				bool isEnum = t.IsEnum;
				bool isGeneric = t.IsGenericType;
				bool isValueType = t.IsValueType;
				bool isClass = t.IsClass;

				if (t.IsGenericType && typeof(ICollection<>).IsAssignableFrom(t.GetGenericTypeDefinition()) ||
					t.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(ICollection<>)) || t.IsClass)
				{
					dto.TrySetMember(p.Name, JsonConvert.SerializeObject(p.GetValue(a, null)));
					if (IsId(p.Name))
						rowKey = p.GetValue(a, null);
				}
				else
				{
					dto.TrySetMember(p.Name, p.GetValue(a, null) == null ? "" : p.GetValue(a, null));
					if (IsId(p.Name))
						rowKey = p.GetValue(a, null);

				}
			}

			if (rowKey == null)
				rowKey = Guid.NewGuid();

			dto.RowKey = rowKey.ToString();
			dto.PartitionKey = partitionKey;
			dto.Timestamp = DateTime.Now;
			dto.IsDirty = true;


			return dto;
		}

		Task<List<T>> StripDTOListAsync(List<Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity> list)
		{
			return Task.Run(() => StripDTOList(list));
		}

		List<T> StripDTOList(List<Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity> list)
		{
			List<T> result = new List<T>();
			foreach (var l in list)
			{
				result.Add(StripDTO(l));
			}

			return result;
		}

		Task<T> StripDTOAsync(Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity a)
		{
			return Task.Run(() => StripDTO(a));
		}

		T StripDTO(Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity a)
		{
			T result = new T();


			Type t1 = result.GetType();
			var dictionary = (IDictionary<string, EntityProperty>)a.Properties;

			foreach (PropertyInfo p1 in t1.GetProperties())//for each property in the entity,
			{
				foreach (var value in dictionary)//see if we have a correspinding property in the DTO
				{
					if (p1.Name == value.Key)
					{
						Type t = p1.PropertyType;
						if (t.IsPrimitive || t == typeof(string))
						{
							p1.SetValue(result, GetValue(value.Value));
						}
						else if (t.IsGenericType && typeof(ICollection<>).IsAssignableFrom(t.GetGenericTypeDefinition()) ||
							t.GetInterfaces().Any(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(ICollection<>)) || t.IsClass)
						{
							var customClass = JsonConvert.DeserializeObject(value.Value.StringValue, t);
							p1.SetValue(result, customClass);
						}
					}
				}

			}

			return result;
		}

		private object GetValue(EntityProperty source)
		{
			switch (source.PropertyType)
			{
				case EdmType.Binary:
					return (object)source.BinaryValue;
				case EdmType.Boolean:
					return (object)source.BooleanValue;
				case EdmType.DateTime:
					return (object)source.DateTimeOffsetValue;
				case EdmType.Double:
					return (object)source.DoubleValue;
				case EdmType.Guid:
					return (object)source.GuidValue;
				case EdmType.Int32:
					return (object)source.Int32Value;
				case EdmType.Int64:
					return (object)source.Int64Value;
				case EdmType.String:
					return (object)source.StringValue.Replace("\"", ""); //remove json text qualifier
				default: throw new TypeLoadException(string.Format("not supported edmType:{0}", source.PropertyType));
			}
		}

		private bool IsId(string candidate)
		{
			bool result = false;

			if (candidate.ToLower() == "id")
				result = true;

			if (candidate.ToLower().Substring(candidate.Length - 2, 2) == "id")
				result = true;

			return result;
		}



		#endregion
	}
}

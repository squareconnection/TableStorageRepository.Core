using System;
using System.Linq;

namespace TableStorageRepository.Core
{
	public static class QueryParser
	{
		public static TableStorageQueryMap GetFilter(string filter)
		{
			TableStorageQueryMap map = new TableStorageQueryMap();
			var raw = filter.Split(' ');

			if (raw.Count() == 3)
			{
				return Map(raw[0], raw[1], raw[2]);
			}

			if (raw.Count() < 3)
			{
				// we have no spaces, so split on operators
				raw = filter.Split('=');
				if (raw.Count() == 2)
				{
					return Map(raw[0], "=", raw[1]);
				}
			}

			return null;

		}

		private static TableStorageQueryMap Map(string propertyName, string operation, string givenValue)
		{

			return new TableStorageQueryMap() { PropertyName = propertyName, GivenValue = givenValue, Operation = ParseOperator(operation) };
		}

		private static string ParseOperator(string source)
		{
			switch (source)
			{
				case "=":
					return QueryComparisonOperator.Equal;
				case "!=":
					return QueryComparisonOperator.NotEqual;
				case "<>":
					return QueryComparisonOperator.NotEqual;
				case "<":
					return QueryComparisonOperator.LessThan;
				case ">":
					return QueryComparisonOperator.GreaterThan;
				case ">=":
					return QueryComparisonOperator.GreaterThanOrEqual;
				case "=>":
					return QueryComparisonOperator.GreaterThanOrEqual;
				case "<=":
					return QueryComparisonOperator.LessThanOrEqual;
				case "=<":
					return QueryComparisonOperator.LessThanOrEqual;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}
	}
	static class QueryComparisonOperator
	{
		//
		// Summary:
		//     Represents the Equal operator.
		public const string Equal = "eq";
		//
		// Summary:
		//     Represents the Not Equal operator.
		public const string NotEqual = "ne";
		//
		// Summary:
		//     Represents the Greater Than operator.
		public const string GreaterThan = "gt";
		//
		// Summary:
		//     Represents the Greater Than or Equal operator.
		public const string GreaterThanOrEqual = "ge";
		//
		// Summary:
		//     Represents the Less Than operator.
		public const string LessThan = "lt";
		//
		// Summary:
		//     Represents the Less Than or Equal operator.
		public const string LessThanOrEqual = "le";
	}
}

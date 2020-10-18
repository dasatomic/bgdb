using PageManager;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace QueryProcessing
{
    internal class RowHolderOrderByComparer : IComparer<RowHolder>
    {
        private readonly OrderByColumn[] columns;

        public RowHolderOrderByComparer(OrderByColumn[] columns)
        {
            this.columns = columns;
        }

        public int Compare([AllowNull] RowHolder x, [AllowNull] RowHolder y)
        {
            foreach (OrderByColumn c in columns)
            {
                int result = Compare(c, x, y);
                if (result != 0) return result;
            }

            return 0;
        }

        private static int Compare(OrderByColumn c, RowHolder x, RowHolder y)
        {
            IComparable left = QueryProcessingAccessors.MetadataColumnRowsetHolderFetcher(c.column, x);
            IComparable right = QueryProcessingAccessors.MetadataColumnRowsetHolderFetcher(c.column, y);

            int result = left.CompareTo(right);
            return (c.direction == OrderByColumn.Direction.Asc) ? result : -result;
        }
    }
}
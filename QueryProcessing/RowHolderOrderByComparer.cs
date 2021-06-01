using PageManager;
using System;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class RowHolderOrderByComparer : IComparer<RowHolder>
    {
        private readonly OrderByColumn[] columns;

        public RowHolderOrderByComparer(OrderByColumn[] columns)
        {
            this.columns = columns;
        }

        public int Compare(RowHolder x, RowHolder y)
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
            return (c.direction == OrderByColumn.Direction.Asc) ? left.CompareTo(right) : right.CompareTo(left);
        }
    }
}
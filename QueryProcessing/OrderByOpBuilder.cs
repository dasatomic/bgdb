using MetadataManager;
using Microsoft.FSharp.Collections;
using PageManager;
using System;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class OrderByOpBuilder : IStatementTreeBuilder
    {
        public Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer)
        {
            if (statement.OrderBy.IsEmpty)
            {
                return Task.FromResult(source);
            }

            OrderByColumn[] orderByColumns = GetOrderByColumns(source.GetOutputColumns(), statement.OrderBy);

            IPhysicalOperator<RowHolder> phyOpOrderBy = new PhyOpOrderBy(source, orderByColumns);
            return Task.FromResult(phyOpOrderBy);
        }

        private OrderByColumn[] GetOrderByColumns(MetadataColumn[] columns, FSharpList<Tuple<string, Sql.dir>> orderBy)
        {
            OrderByColumn[] orderByColumns = new OrderByColumn[orderBy.Length];
            for(int i = 0; i < orderBy.Length; ++i)
            {
                var column = QueryProcessingAccessors.GetMetadataColumn(orderBy[i].Item1, columns);
                OrderByColumn.Direction direction = orderBy[i].Item2 == Sql.dir.Asc ? OrderByColumn.Direction.Asc : OrderByColumn.Direction.Desc;

                orderByColumns[i] = new OrderByColumn(column, direction);
            }

            return orderByColumns;
        }
    }
}
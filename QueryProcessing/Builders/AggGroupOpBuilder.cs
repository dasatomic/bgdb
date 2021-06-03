using PageManager;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class AggGroupOpBuilder : IStatementTreeBuilder
    {
        public Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer)
        {
            Sql.columnSelect[] columns = new Sql.columnSelect[0];
            Tuple<Sql.aggType, string>[] aggregates = new Tuple<Sql.aggType, string>[0];

            if (!statement.Columns.IsStar)
            {
                columns = (((Sql.selectType.ColumnList)statement.Columns).Item).ToArray();

                aggregates = columns
                    .Where(c => c.IsAggregate == true)
                    .Select(c => ((Sql.columnSelect.Aggregate)c).Item).ToArray();
            }


            if (statement.GroupBy.Any() || aggregates.Any())
            {
                string[] groupByColumns = statement.GroupBy.ToArray();

                GroupByFunctors groupByFunctors = GroupByStatementBuilder.EvalGroupBy(groupByColumns, columns, source.GetOutputColumns());
                IPhysicalOperator<RowHolder> phyOpGroupBy = new PhyOpGroupBy(source, groupByFunctors);
                return Task.FromResult(phyOpGroupBy);

            } else
            {
                return Task.FromResult(source);
            }
        }
    }
}

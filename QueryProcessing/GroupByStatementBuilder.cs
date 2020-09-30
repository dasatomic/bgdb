using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QueryProcessing
{
    public class GroupByFunctors
    {
        /// <summary>
        /// Grouper function is responsible to push items into
        /// buckets. Bucket is projection by group by columns.
        /// </summary>
        public Func<RowHolder, RowHolder> Grouper { get; }

        /// <summary>
        /// List of functors that are to be applied for aggregation.
        /// Input is RowHolder, state and output is new state of aggregation.
        /// </summary>
        public Func<RowHolder, RowHolder, RowHolder> Aggregate { get; }

        /// <summary>
        /// Projects is union of columns from group by and aggregate.
        /// </summary>
        public Func<RowHolder, RowHolder> Projector { get; }

        public GroupByFunctors(
            Func<RowHolder, RowHolder> projector,
            Func<RowHolder, RowHolder> grouper,
            Func<RowHolder, RowHolder, RowHolder> aggs)
        {
            this.Projector = projector;
            this.Grouper = grouper;
            this.Aggregate = aggs;
        }
    }

    class GroupByStatementBuilder
    {
        public static GroupByFunctors EvalGroupBy(string[] groupByColumns, Sql.columnSelect[] selectColumns, MetadataColumn[] metadataColumns)
        {
            foreach (string groupBy in groupByColumns)
            {
                if (!metadataColumns.Any(mc => mc.ColumnName == groupBy))
                {
                    throw new KeyNotFoundException($"Unknown column {groupBy}");
                }
            }

            string[] projectColumnNames = selectColumns
                .Select(c =>
                {
                    if (c.IsAggregate)
                    {
                        return ((Sql.columnSelect.Aggregate)c).Item.Item2;
                    }
                    else if (c.IsProjection)
                    {
                        return ((Sql.columnSelect.Projection)c).Item;
                    }
                    else
                    {
                        throw new InvalidProgramException("Invalid state");
                    }
                }).ToArray();

            Tuple<Sql.aggType, string>[] aggregators = selectColumns
                .Where(c => c.IsAggregate == true)
                .Select(c => ((Sql.columnSelect.Aggregate)c).Item).ToArray();

            if (aggregators.Select(agg => agg.Item2).Except(metadataColumns.Select(mc => mc.ColumnName)).Any())
            {
                throw new KeyNotFoundException($"Can't find columns in agg.");
            }

            int[] projectColumnPosition = projectColumnNames.Select(col => metadataColumns.First(mc => mc.ColumnName == col).ColumnId).ToArray();

            MetadataColumn[] mdColumnsForAggs = new MetadataColumn[aggregators.Length];
            MetadataColumn[] mdColumnsForGroupBy = new MetadataColumn[groupByColumns.Length];

            int posInAggs = 0;
            int posInGroupBy = 0;

            foreach (Sql.columnSelect column in selectColumns)
            {
                if (column.IsAggregate)
                {
                    Tuple<Sql.aggType, string> agg = ((Sql.columnSelect.Aggregate)column).Item;
                    mdColumnsForAggs[posInAggs] = metadataColumns.First(metadataColumns => metadataColumns.ColumnName == agg.Item2);

                    // Relative position.
                    mdColumnsForAggs[posInAggs].ColumnId = posInAggs + posInGroupBy;

                    posInAggs++;
                }
                else if (column.IsProjection)
                {
                    string groupby = ((Sql.columnSelect.Projection)column).Item;
                    mdColumnsForGroupBy[posInGroupBy] = metadataColumns.First(metadataColumns => metadataColumns.ColumnName == groupby);

                    // Group by is taking from source op. No need for relative positions.

                    posInGroupBy++;
                }
                else
                {
                    throw new InvalidProgramException();
                }
            }

            // TODO: Aiming for correctness. Perf comes later.
            Func<RowHolder, RowHolder> projector = (rowHolder) =>
            {
                return rowHolder.Project(projectColumnPosition);
            };

            Func<RowHolder, RowHolder> grouper = (rowHolder) =>
            {
                if (!groupByColumns.Any())
                {
                    return RowHolder.Zero();
                }
                else
                {
                    return rowHolder.Project(mdColumnsForGroupBy.Select(x => x.ColumnId).ToArray());
                }
            };

            Func<RowHolder /* Current Row, after project */, RowHolder /* Current state */, RowHolder /* New state */> aggregator =
                (inputRhf, stateRhf) =>
                {
                    int pos = 0;
                    foreach (Tuple<Sql.aggType, string> agg in aggregators)
                    {
                        QueryProcessingAccessors.ApplyAgg(mdColumnsForAggs[pos], ref inputRhf, agg.Item1, ref stateRhf);
                        pos++;
                    }

                    return stateRhf;
                };

            return new GroupByFunctors(projector, grouper, aggregator);
        }
    }
}

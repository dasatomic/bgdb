using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace QueryProcessing
{
    public class GroupByFunctors
    {
        /// <summary>
        /// Grouper function is responsible to push items into
        /// buckets. Bucket is projection by group by columns.
        /// </summary>
        public Func<RowHolderFixed, RowHolderFixed> Grouper { get; }

        /// <summary>
        /// List of functors that are to be applied for aggregation.
        /// Input is RowHolder, state and output is new state of aggregation.
        /// </summary>
        public Func<RowHolderFixed, RowHolderFixed, RowHolderFixed> Aggregate { get; }

        /// <summary>
        /// Projects is union of columns from group by and aggregate.
        /// </summary>
        public Func<RowHolderFixed, RowHolderFixed> Projector { get; }

        public GroupByFunctors(
            Func<RowHolderFixed, RowHolderFixed> projector,
            Func<RowHolderFixed, RowHolderFixed> grouper,
            Func<RowHolderFixed, RowHolderFixed, RowHolderFixed> aggs)
        {
            this.Projector = projector;
            this.Grouper = grouper;
            this.Aggregate = aggs;
        }
    }

    class GroupByStatementBuilder
    {
        public static GroupByFunctors EvalGroupBy(string[] groupByColumns, Tuple<Sql.aggType, string>[] aggregators, MetadataColumn[] metadataColumns)
        {
            if (aggregators.Select(agg => agg.Item2).Except(metadataColumns.Select(mc => mc.ColumnName)).Any())
            {
                throw new KeyNotFoundException($"Can't find columns in agg.");
            }

            // TODO: Can't use column id as column position fetcher.
            int[] mdInGroupBy = metadataColumns.Where(mc => groupByColumns.Contains(mc.ColumnName)).Select(mc => mc.ColumnId).ToArray();
            int[] mdInAgg = aggregators.Select(agg => metadataColumns.First(mc => mc.ColumnName == agg.Item2).ColumnId).ToArray();

            Debug.Assert(!Enumerable.Intersect(mdInGroupBy, mdInAgg).Any());

            int[] columnUnion = new int[mdInGroupBy.Length + mdInAgg.Length];
            for (int i = 0; i < mdInGroupBy.Length; i++)
            {
                columnUnion[i] = mdInGroupBy[i];
            }

            for (int i = 0; i < mdInAgg.Length; i++)
            {
                columnUnion[i + mdInGroupBy.Length] = mdInAgg[i];
            }

            if (mdInGroupBy.Length != groupByColumns.Length)
            {
                string message = string.Join(',', groupByColumns.Except(metadataColumns.Select(mc => mc.ColumnName)));
                throw new KeyNotFoundException($"Can't find columns in group by {message}");
            }

            MetadataColumn[] mdColumnsForAggs = new MetadataColumn[mdInAgg.Length];

            int pos = 0;
            foreach (Tuple<Sql.aggType, string> agg in aggregators)
            {
                mdColumnsForAggs[pos] = metadataColumns.First(metadataColumns => metadataColumns.ColumnName == agg.Item2);

                // Need to align with new position of this column after project.
                // TODO: This should be handled in more systematic way.
                // e.g. by building logical tree from ast.
                mdColumnsForAggs[pos].ColumnId = mdInGroupBy.Length + pos;

                pos++;
            }

            // TODO: Aiming for correctness. Perf comes later.
            Func<RowHolderFixed, RowHolderFixed> projector = (rowHolder) =>
            {
                return rowHolder.Project(columnUnion);
            };

            Func<RowHolderFixed, RowHolderFixed> grouper = (rowHolder) =>
            {
                if (!mdInGroupBy.Any())
                {
                    return RowHolderFixed.Zero();
                }
                else
                {
                    return rowHolder.Project(mdInGroupBy);
                }
            };

            Func<RowHolderFixed /* Current Row, after project */, RowHolderFixed /* Current state */, RowHolderFixed /* New state */> aggregator =
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

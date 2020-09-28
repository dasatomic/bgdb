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
            // TODO: Aiming for correctness. Perf comes later.
            Func<RowHolderFixed, RowHolderFixed> projector = (rowHolder) =>
            {
                // TODO: Can't use column id as column position fetcher.
                int[] mdInGroupBy = metadataColumns.Where(mc => groupByColumns.Contains(mc.ColumnName)).Select(mc => mc.ColumnId).ToArray();
                int[] mdInAgg = metadataColumns.Where(mc => aggregators.Select(agg => agg.Item2).Contains(mc.ColumnName)).Select(mc => mc.ColumnId).ToArray();

                Debug.Assert(!Enumerable.Intersect(mdInGroupBy, mdInAgg).Any());

                // Grouper extracts both group by part and aggregate part.
                return rowHolder.Project(mdInGroupBy.Union(mdInAgg).ToArray());
            };

            Func<RowHolderFixed, RowHolderFixed> grouper = (rowHolder) =>
            {
                // TODO: Can't use column id as column position fetcher.
                int[] mdInGroupBy = metadataColumns.Where(mc => groupByColumns.Contains(mc.ColumnName)).Select(mc => mc.ColumnId).ToArray();

                // Grouper extracts both group by part and aggregate part.
                return rowHolder.Project(mdInGroupBy);
            };

            Func<RowHolderFixed /* Current Row */, RowHolderFixed /* Current state */, RowHolderFixed /* New state */> aggregator =
                (inputRhf, stateRhf) =>
                {
                    foreach (Tuple<Sql.aggType, string> agg in aggregators)
                    {
                        MetadataColumn mc = metadataColumns.First(mc => mc.ColumnName == agg.Item2);

                        QueryProcessingAccessors.ApplyAgg(mc, ref inputRhf, agg.Item1, ref stateRhf);

                    }

                    return stateRhf;
                };

            return new GroupByFunctors(projector, grouper, aggregator);
        }
    }
}

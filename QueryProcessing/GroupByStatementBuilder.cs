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

        public MetadataColumn[] ProjectColumnInfo { get; }

        public GroupByFunctors(
            Func<RowHolder, RowHolder> projector,
            Func<RowHolder, RowHolder> grouper,
            Func<RowHolder, RowHolder, RowHolder> aggs,
            MetadataColumn[] projectColumnInfo)
        {
            this.Projector = projector;
            this.Grouper = grouper;
            this.Aggregate = aggs;
            this.ProjectColumnInfo = projectColumnInfo;
        }
    }

    class GroupByStatementBuilder
    {
        public static GroupByFunctors EvalGroupBy(string[] groupByColumns, Sql.columnSelect[] selectColumns, MetadataColumn[] metadataColumns)
        {
            // Just prototyping for now. This needs a lot of refactoring.

            foreach (string groupByColumn in groupByColumns)
            {
                // Just checking whether all columns have correct name.
                var _ = QueryProcessingAccessors.GetMetadataColumn(groupByColumn, metadataColumns);
            }

            // metadata column, is extended.
            (MetadataColumn, bool)[] projectColumns = selectColumns
                .Select(c =>
                {
                    if (c.IsAggregate)
                    {
                        var agg = ((Sql.columnSelect.Aggregate)c).Item;
                        MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(agg.Item2, metadataColumns);

                        if (agg.Item1.IsCount)
                        {
                            // For count we need to update return type to int.
                            mc = new MetadataColumn(mc.ColumnId, mc.TableId, mc.ColumnName + "_Count", new ColumnInfo(ColumnType.Int));
                            return (mc, true);
                        }
                        else if (agg.Item1.IsMax)
                        {
                            mc = new MetadataColumn(mc.ColumnId, mc.TableId, mc.ColumnName + "_Max", mc.ColumnType);
                        }
                        else if (agg.Item1.IsMin)
                        {
                            mc = new MetadataColumn(mc.ColumnId, mc.TableId, mc.ColumnName + "_Min", mc.ColumnType);
                        }
                        else if (agg.Item1.IsSum)
                        {
                            mc = new MetadataColumn(mc.ColumnId, mc.TableId, mc.ColumnName + "_Sum", mc.ColumnType);
                        }

                        return (mc, false);
                    }
                    else if (c.IsProjection)
                    {
                        string projection = ((Sql.columnSelect.Projection)c).Item;
                        MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(projection, metadataColumns);
                        return (mc, false);
                    }
                    else
                    {
                        throw new InvalidProgramException("Invalid state");
                    }
                }).ToArray();

            // Need to find all count aggregates.
            List<int> countPositions = new List<int>();
            for (int countPos = 0; countPos < selectColumns.Length; countPos++)
            {
                if (selectColumns[countPos].IsAggregate)
                {
                    if (((Sql.columnSelect.Aggregate)selectColumns[countPos]).Item.Item1.IsCount)
                    {
                        countPositions.Add(countPos);
                    }
                }
            }

            Tuple<Sql.aggType, string>[] aggregators = selectColumns
                .Where(c => c.IsAggregate == true)
                .Select(c => ((Sql.columnSelect.Aggregate)c).Item).ToArray();

            (int?, ColumnInfo?)[] projectExtendInfo = projectColumns.Select<(MetadataColumn, bool), (int?, ColumnInfo?)>(pc =>
            {
                if (pc.Item2 == false)
                {
                    return (pc.Item1.ColumnId, null);
                }
                else
                {
                    return (null, pc.Item1.ColumnType);
                }
            }).ToArray();

            MetadataColumn[] mdColumnsForAggs = new MetadataColumn[aggregators.Length];
            MetadataColumn[] mdColumnsForGroupBy = new MetadataColumn[groupByColumns.Length];

            int posInAggs = 0;
            int posInGroupBy = 0;

            foreach (Sql.columnSelect column in selectColumns)
            {
                if (column.IsAggregate)
                {
                    Tuple<Sql.aggType, string> agg = ((Sql.columnSelect.Aggregate)column).Item;

                    mdColumnsForAggs[posInAggs] = QueryProcessingAccessors.GetMetadataColumn(agg.Item2, metadataColumns);

                    if (agg.Item1.IsCount)
                    {
                        // Relative position + for count change type to int.
                        mdColumnsForAggs[posInAggs] = new MetadataColumn(
                            posInAggs + posInGroupBy,
                            mdColumnsForAggs[posInAggs].TableId,
                            mdColumnsForAggs[posInAggs].ColumnName,
                            new ColumnInfo(ColumnType.Int));
                    }
                    else
                    {
                        // Relative position.
                        mdColumnsForAggs[posInAggs] = new MetadataColumn(
                            posInAggs + posInGroupBy,
                            mdColumnsForAggs[posInAggs].TableId,
                            mdColumnsForAggs[posInAggs].ColumnName,
                            mdColumnsForAggs[posInAggs].ColumnType);
                    }

                    posInAggs++;
                }
                else if (column.IsProjection)
                {
                    string groupby = ((Sql.columnSelect.Projection)column).Item;
                    mdColumnsForGroupBy[posInGroupBy] = QueryProcessingAccessors.GetMetadataColumn(groupby, metadataColumns);

                    // Group by is taking from source op. No need for relative positions.
                    posInGroupBy++;
                }
                else
                {
                    throw new InvalidProgramException();
                }
            }

            Func<RowHolder, RowHolder> projector = (rowHolder) =>
            {
                RowHolder res = rowHolder.ProjectAndExtend(projectExtendInfo);

                // init all counts to 1.
                foreach (int pos in countPositions)
                {
                    res.SetField<int>(pos, 1);
                }

                return res;
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

            return new GroupByFunctors(projector, grouper, aggregator, projectColumns.Select(pc => pc.Item1).ToArray());
        }
    }
}

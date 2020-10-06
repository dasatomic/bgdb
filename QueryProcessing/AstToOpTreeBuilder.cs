using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using QueryProcessing.Exceptions;

namespace QueryProcessing
{
    /// <summary>
    /// Pair of async enumerable, that can yield rows from root op of query tree and columns of root op.
    /// </summary>
    public class RowProvider
    {
        public RowProvider(IAsyncEnumerable<RowHolder> enumerator, MetadataColumn[] columnInfo)
        {
            this.Enumerator = enumerator;
            this.ColumnInfo = columnInfo;
        }

        public IAsyncEnumerable<RowHolder> Enumerator { get; }
        public MetadataColumn[] ColumnInfo { get; }
    }

    public class AstToOpTreeBuilder
    {
        private MetadataManager.MetadataManager metadataManager;

        public AstToOpTreeBuilder(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }


        public async Task<RowProvider> ParseSqlStatement(Sql.sqlStatement sqlStatement, ITransaction tran, InputStringNormalizer stringNormalizer)
        {
            // TODO: query builder is currently manual. i.e. SCAN -> optional(FILTER) -> PROJECT.
            // In future we need to build proper algebrizer, relational algebra rules and work on QO.
            string tableName = sqlStatement.Table;

            string[] projections = new string[0];
            Sql.columnSelect[] columns = new Sql.columnSelect[0];
            Tuple<Sql.aggType, string>[] aggregates = new Tuple<Sql.aggType, string>[0];
            bool isStar = false;

            if (sqlStatement.Columns.IsStar)
            {
                isStar = true;
            }
            else
            {
                columns = (((Sql.selectType.ColumnList)sqlStatement.Columns).Item).ToArray();

                projections = columns
                    .Where(c => c.IsProjection == true)
                    .Select(c => ((Sql.columnSelect.Projection)c).Item).ToArray();

                aggregates = columns
                    .Where(c => c.IsAggregate == true)
                    .Select(c => ((Sql.columnSelect.Aggregate)c).Item).ToArray();
            }


            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            // Scan Op.
            PhyOpScan scanOp = new PhyOpScan(table.Collection, tran, table.Columns);

            // Where op.
            IPhysicalOperator<RowHolder> sourceForProject = scanOp;

            if (FSharpOption<Sql.where>.get_IsSome(sqlStatement.Where))
            {
                Sql.where whereStatement = sqlStatement.Where.Value;
                PhyOpFilter filterOp = new PhyOpFilter(scanOp, FilterStatementBuilder.EvalWhere(whereStatement, table.Columns, stringNormalizer));
                sourceForProject = filterOp;
            }

            if (sqlStatement.GroupBy.Any() || aggregates.Any())
            {
                string[] groupByColumns = sqlStatement.GroupBy.ToArray();

                GroupByFunctors groupByFunctors = GroupByStatementBuilder.EvalGroupBy(groupByColumns, columns, table.Columns);
                PhyOpGroupBy phyOpGroupBy = new PhyOpGroupBy(sourceForProject, groupByFunctors);

                return new RowProvider(phyOpGroupBy.Iterate(tran), groupByFunctors.ProjectColumnInfo);
            } else
            {
                if (isStar)
                {
                    // no need for project, just return everything.
                    return new RowProvider(sourceForProject.Iterate(tran), sourceForProject.GetOutputColumns());
                }

                // Project Op.
                List<MetadataColumn> columnMapping = new List<MetadataColumn>();
                foreach (string columnName in projections)
                {
                    if (!table.Columns.Any(tbl => tbl.ColumnName == columnName))
                    {

                        throw new KeyNotFoundException(string.Format("Invalid column name {0}", columnName));
                    }

                    columnMapping.Add(table.Columns.FirstOrDefault(c => c.ColumnName == columnName));
                }

                PhyOpProject projectOp = new PhyOpProject(sourceForProject, columnMapping.Select(mc => mc.ColumnId).ToArray());

                return new RowProvider(projectOp.Iterate(tran), columnMapping.ToArray());
            }
        }

        public async Task<PhyOpTableInsert> ParseInsertStatement(Sql.insertStatement insertStatement, ITransaction tran, InputStringNormalizer stringNormalizer)
        {
            string tableName = insertStatement.Table;

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            ColumnInfo[] columnInfosFromTable = table.Columns.Select(mt => mt.ColumnType).ToArray();

            RowHolder rowHolder = new RowHolder(columnInfosFromTable);

            int colNum = 0;
            foreach (var value in insertStatement.Values)
            {
                if (value.IsFloat)
                {
                    if (columnInfosFromTable[colNum].ColumnType == ColumnType.Double)
                    {
                        rowHolder.SetField<double>(colNum, ((Sql.value.Float)value).Item);
                    }
                    else
                    {
                        throw new InvalidColumnTypeException();
                    }
                }
                else if (value.IsInt)
                {
                    if (columnInfosFromTable[colNum].ColumnType == ColumnType.Int)
                    {
                        rowHolder.SetField<int>(colNum, ((Sql.value.Int)value).Item);
                    }
                    else if (columnInfosFromTable[colNum].ColumnType == ColumnType.Double)
                    {
                        // Int can be cast to double without data loss.
                        rowHolder.SetField<double>(colNum, (double)((Sql.value.Int)value).Item);
                    }
                    else
                    {
                        throw new InvalidColumnTypeException();
                    }
                }
                else if (value.IsString)
                {
                    if (columnInfosFromTable[colNum].ColumnType == ColumnType.String)
                    {
                        // TODO: For string heap (strings of variable length separate logic is needed.

                        string input = ((Sql.value.String)value).Item;
                        input = stringNormalizer.ApplyReplacementTokens(input);
                        rowHolder.SetField(colNum, input.ToCharArray());
                    }
                    else
                    {
                        throw new InvalidColumnTypeException();
                    }
                }
                else { throw new ArgumentException(); }

                colNum++;
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(rowHolder);

            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic);
            return op;
        }
    }
}

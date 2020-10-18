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
        private IList<IStatementTreeBuilder> statementBuildersList = new List<IStatementTreeBuilder>();

        public AstToOpTreeBuilder(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
            statementBuildersList.Add(new SourceOpBuilder(metadataManager));
            statementBuildersList.Add(new JoinOpBuilder(metadataManager));
            statementBuildersList.Add(new FilterStatementBuilder());
            statementBuildersList.Add(new AggGroupOpBuilder());
            statementBuildersList.Add(new OrderByOpBuilder());
            statementBuildersList.Add(new ProjectOpBuilder());
        }


        public async Task<RowProvider> ParseSqlStatement(Sql.sqlStatement sqlStatement, ITransaction tran, InputStringNormalizer stringNormalizer)
        {
            // TODO: query builder is currently manual. i.e. SCAN -> optional(JOINS) -> optional(FILTER) -> GROUP BY -> ORDER BY/PROJECT.
            // In future we need to build proper algebrizer, relational algebra rules and work on QO.

            IPhysicalOperator<RowHolder> source = null;
            foreach (IStatementTreeBuilder builder in this.statementBuildersList)
            {
                source = await builder.BuildStatement(sqlStatement, tran, source, stringNormalizer);
            }

            return new RowProvider(source.Iterate(tran), source.GetOutputColumns());
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

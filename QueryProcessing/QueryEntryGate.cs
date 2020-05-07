using FSharp.Text.Lexing;
using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface IExecuteQuery
    {
        Task<Row[]> Execute(string queryText);
        Task ExecuteDdl(string queryText);
    }

    public class QueryEntryGate : IExecuteQuery
    {
        private AstToOpTreeBuilder treeBuilder;
        private MetadataManager.MetadataManager metadataManager;

        public QueryEntryGate(MetadataManager.MetadataManager metadataManager, IAllocateMixedPage allocator, HeapWithOffsets<char[]> stringHeap)
        {
            this.metadataManager = metadataManager;
            treeBuilder = new AstToOpTreeBuilder(metadataManager, stringHeap, allocator);
        }

        public async Task<Row[]> Execute(string queryText)
        {
            var lexbuf = LexBuffer<char>.FromString(queryText);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);

            if (!statement.IsSelect)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Select selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement);

            IPhysicalOperator<Row> rootOp = this.treeBuilder.ParseSqlStatement(selectStatement.Item);
            return rootOp.ToArray();
        }

        public async Task ExecuteDdl(string queryText)
        {
            var lexbuf = LexBuffer<char>.FromString(queryText);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            Sql.DmlDdlSqlStatement statement = SqlParser.startCT(FuncConvert.FromFunc(func), lexbuf);

            if (statement.IsCreate)
            {
                Sql.DmlDdlSqlStatement.Create createStatement = (Sql.DmlDdlSqlStatement.Create)statement;
                string tableName = createStatement.Item.Table;
                var columns = createStatement.Item.ColumnList.ToList();

                MetadataTablesManager tableManager = this.metadataManager.GetTableManager();

                TableCreateDefinition tableCreateDefinition = new TableCreateDefinition();
                tableCreateDefinition.TableName = tableName;
                tableCreateDefinition.ColumnNames = columns.Select(c => c.Item2).ToArray();
                tableCreateDefinition.ColumnTypes = columns.Select(c =>
                {
                    if (c.Item1.IsDoubleCType) return ColumnType.Double;
                    else if (c.Item1.IsIntCType) return ColumnType.Int;
                    else if (c.Item1.IsStringCType) return ColumnType.StringPointer;
                    else throw new ArgumentException();
                }).ToArray();

                tableManager.CreateObject(tableCreateDefinition);
            }
        }
    }
}

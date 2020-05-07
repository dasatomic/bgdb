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
            Sql.sqlStatement statement = SqlParser.start(FuncConvert.FromFunc(func), lexbuf);

            IPhysicalOperator<Row> rootOp = this.treeBuilder.ParseSqlStatement(statement);
            return rootOp.ToArray();
        }

        public async Task ExecuteDdl(string queryText)
        {
            var lexbuf = LexBuffer<char>.FromString(queryText);
            Func<LexBuffer<char>, CreateTableParser.token> func = (x) => CreateTableLexer.tokenize(x);
            Sql.createTableStatement statement = CreateTableParser.startCT(FuncConvert.FromFunc(func), lexbuf);

            string tableName = statement.Table;
            var columns = statement.ColumnList.ToList();

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

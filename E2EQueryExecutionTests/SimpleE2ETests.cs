using FSharp.Text.Lexing;
using MetadataManager;
using Microsoft.FSharp.Core;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Linq;

namespace E2EQueryExecutionTests
{
    public class Tests
    {
        [Test]
        public void CreateTableE2E()
        {
            var allocator = new InMemoryPageManager(4096);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);

            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(mm, stringHeap, allocator);

            string query = @"CREATE TABLE MyTable (INT a, DOUBLE b, STRING c)";
            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, CreateTableParser.token> func = (x) => CreateTableLexer.tokenize(x);
            var f = FuncConvert.FromFunc(func);
            Sql.createTableStatement statement = CreateTableParser.startCT(f, lexbuf);

            treeBuilder.ParseDdl(statement);

            var tm = mm.GetTableManager();
            var table = tm.GetByName("MyTable");

            Assert.NotNull(table);
        }

        [Test]
        public void SimpleE2E()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
            var tm = mm.GetTableManager();

            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
            });

            var table = tm.GetById(id);

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };
            string query =
                @"SELECT a, b, c
                FROM Table";

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic);
            op.Invoke();

            var lexbuf = LexBuffer<char>.FromString(query);
            Func<LexBuffer<char>, SqlParser.token> func = (x) => SqlLexer.tokenize(x);
            var f = FuncConvert.FromFunc(func);
            Sql.sqlStatement statement = SqlParser.start(f, lexbuf);

            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(mm, stringHeap, allocator);
            IPhysicalOperator<Row> root =  treeBuilder.ParseSqlStatement(statement);

            Row[] result = root.ToArray();

            Assert.AreEqual(source, result);
        }
    }
}
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class Tests
    {
        [Test]
        public async Task CreateTableE2E()
        {
            var allocator = new InMemoryPageManager(4096);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);

            IExecuteQuery queryExecutor = new QueryEntryGate(mm, allocator, stringHeap);

            string query = @"CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await queryExecutor.ExecuteDdl(query);

            var tm = mm.GetTableManager();
            var table = tm.GetByName("MyTable");

            Assert.NotNull(table);
        }

        [Test]
        public async Task SimpleE2E()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
            IExecuteQuery queryExecutor = new QueryEntryGate(mm, allocator, stringHeap);
            var tm = mm.GetTableManager();

            string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_STRING b, TYPE_DOUBLE c)";
            await queryExecutor.ExecuteDdl(createTableQuery);

            // TODO: Remove insert once you have parser support.
            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            var table = tm.GetByName("Table");

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic);
            op.Invoke();

            string query = @"SELECT a, b, c FROM Table";
            Row[] result = await queryExecutor.Execute(query);

            Assert.AreEqual(source, result);
        }
    }
}
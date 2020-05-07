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
            await queryExecutor.ExecuteNonQuery(query);

            var tm = mm.GetTableManager();
            var table = tm.GetByName("MyTable");

            Assert.NotNull(table);
        }

        [Test]
        public async Task SimpleE2E()
        {
            var allocator = new InMemoryPageManager(4096);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator);

            IExecuteQuery queryExecutor = new QueryEntryGate(
                new MetadataManager.MetadataManager(allocator, stringHeap, allocator),
                allocator,
                stringHeap);

            string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await queryExecutor.ExecuteNonQuery(createTableQuery);

            string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
            await queryExecutor.ExecuteNonQuery(insertQuery);

            string query = @"SELECT a, b, c FROM Table";
            Row[] result = await queryExecutor.Execute(query);

            Assert.AreEqual(1, result[0].IntCols[0]);
            Assert.AreEqual(1.1, result[0].DoubleCols[0]);
            Assert.AreEqual("mystring", result[0].StringCols[0]);
        }
    }
}
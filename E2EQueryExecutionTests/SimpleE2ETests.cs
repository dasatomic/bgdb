using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class Tests
    {
        private QueryEntryGate queryEntryGate;

        [SetUp]
        public void Setup()
        {
            var allocator = new InMemoryPageManager(4096);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(mm, stringHeap, allocator);

            this.queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(mm),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });
        }

        [Test]
        public async Task CreateTableE2E()
        {
            string query = @"CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await this.queryEntryGate.Execute(query);
        }

        [Test]
        public async Task SimpleE2E()
        {

            string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await this.queryEntryGate.Execute(createTableQuery);

            string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
            await this.queryEntryGate.Execute(insertQuery);

            string query = @"SELECT a, b, c FROM Table";
            Row[] result = (await this.queryEntryGate.Execute(query)).ToArray();

            Assert.AreEqual(1, result[0].IntCols[0]);
            Assert.AreEqual(1.1, result[0].DoubleCols[0]);
            Assert.AreEqual("mystring", result[0].StringCols[0]);
        }
    }
}
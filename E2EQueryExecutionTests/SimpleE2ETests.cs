using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class Tests
    {
        private QueryEntryGate queryEntryGate;
        private ILogManager logManager;

        [SetUp]
        public async Task Setup()
        {
            var allocator = new InMemoryPageManager(4096);
            this.logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            using (Transaction tran = new Transaction(logManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(allocator, tran);
                await tran.Commit();
            }

            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
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
            using (Transaction tran = new Transaction(logManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran);
                await tran.Commit();
            }
        }

        [Test]
        public async Task SimpleE2E()
        {
            using (Transaction tran = new Transaction(logManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran);
                await tran.Commit();
            }

            using (Transaction tran = new Transaction(logManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
                await this.queryEntryGate.Execute(insertQuery, tran);

                insertQuery = "INSERT INTO Table VALUES (2, 2.2, mystring2)";
                await this.queryEntryGate.Execute(insertQuery, tran);
                await tran.Commit();
            }


            using (Transaction tran = new Transaction(logManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM Table";
                Row[] result = (await this.queryEntryGate.Execute(query, tran)).ToArray();

                Assert.AreEqual(1, result[0].IntCols[0]);
                Assert.AreEqual(1.1, result[0].DoubleCols[0]);
                Assert.AreEqual("mystring", result[0].StringCols[0]);

                Assert.AreEqual(2, result[1].IntCols[0]);
                Assert.AreEqual(2.2, result[1].DoubleCols[0]);
                Assert.AreEqual("mystring2", result[1].StringCols[0]);

                // TODO: If this is readonly tran I don't need to commit.
                // It should be enough to just relese the resources.
                await tran.Commit();
            }
        }
    }
}
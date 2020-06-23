using DataStructures;
using LockManager;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace E2EQueryExecutionTests
{
    public class Tests
    {
        private QueryEntryGate queryEntryGate;
        private ILogManager logManager;
        private IPageManager pageManager;
        private MetadataManager.MetadataManager metadataManager;

        [SetUp]
        public async Task Setup()
        {
            this.pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            this.logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            await using (Transaction tran = new Transaction(logManager, pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager, stringHeap, pageManager);

            this.queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });
        }

        [Test]
        public async Task CreateTableE2E()
        {
            string query = @"CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();
            }
        }

        [Test]
        public async Task CreateTableE2ERollback()
        {
            string query = @"CREATE TABLE MyTableRollback (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToListAsync();
                await tran.Rollback();
            }

            var tableManager = this.metadataManager.GetTableManager();
            await using (Transaction tran = new Transaction(logManager, pageManager, "CHECK_TABLE"))
            {
                Assert.IsFalse(await tableManager.Iterate(tran).AnyAsync());
            }

            query = @"CREATE TABLE MyTableCommit (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToListAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "CHECK_TABLE"))
            {
                Assert.AreEqual(1, tableManager.Iterate(tran).ToEnumerable().Count());
            }
        }

        [Test]
        public async Task SimpleE2E()
        {
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();

                insertQuery = "INSERT INTO Table VALUES (2, 2.2, mystring2)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM Table";
                Row[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(1, result[0].IntCols[0]);
                Assert.AreEqual(1.1, result[0].DoubleCols[0]);
                Assert.AreEqual("mystring", result[0].StringCols[0]);

                Assert.AreEqual(2, result[1].IntCols[0]);
                Assert.AreEqual(2.2, result[1].DoubleCols[0]);
                Assert.AreEqual("mystring2", result[1].StringCols[0]);

                await tran.Commit();
            }
        }

        [Test]
        public async Task E2EWithRollback()
        {
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();

                insertQuery = "INSERT INTO Table VALUES (2, 2.2, mystring2)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Rollback();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (2, 2.2, mystring2)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM Table";
                Row[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(1, result[0].IntCols[0]);
                Assert.AreEqual(1.1, result[0].DoubleCols[0]);
                Assert.AreEqual("mystring", result[0].StringCols[0]);

                Assert.AreEqual(2, result[1].IntCols[0]);
                Assert.AreEqual(2.2, result[1].DoubleCols[0]);
                Assert.AreEqual("mystring2", result[1].StringCols[0]);

                await tran.Commit();
            }
        }

        [Test]
        public async Task E2EBufferPoolExceed()
        {
            BufferPool bp = new BufferPool();

            IPageEvictionPolicy restrictiveEviction = new FifoEvictionPolicy(3, 1);
            ILockManager lm = new LockManager.LockManager();
            this.pageManager =  new PageManager.PageManager(4096, restrictiveEviction, TestGlobals.DefaultPersistedStream, bp, lm);

            const int rowInsert = 5000;

            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE LargeTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "INSERT"))
            {
                for (int i = 0; i < rowInsert; i++)
                {
                    string insertQuery = $"INSERT INTO LargeTable VALUES ({i}, {i}.1, mystring{i})";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                }

                await tran.Commit();
            }

            await using (Transaction tran = new Transaction(logManager, pageManager, "SELECT"))
            {
                string query = @"SELECT a, b, c FROM LargeTable";
                Row[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(rowInsert, result.Length);
            }
        }
    }
}
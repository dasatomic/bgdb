using DataStructures;
using LockManager;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Collections.Generic;
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

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "SETUP"))
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
            string query = @"CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();
            }
        }

        [Test]
        public async Task CreateTableE2ERollback()
        {
            string query = @"CREATE TABLE MyTableRollback (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToListAsync();
                await tran.Rollback();
            }

            var tableManager = this.metadataManager.GetTableManager();
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CHECK_TABLE"))
            {
                Assert.IsFalse(await tableManager.Iterate(tran).AnyAsync());
            }

            query = @"CREATE TABLE MyTableCommit (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                await this.queryEntryGate.Execute(query, tran).ToListAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CHECK_TABLE"))
            {
                Assert.AreEqual(1, tableManager.Iterate(tran).ToEnumerable().Count());
            }
        }

        [Test]
        public async Task SimpleE2E()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, 'mystring')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();

                insertQuery = "INSERT INTO Table VALUES (2, 2.2, 'mystring2')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM Table";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(1, result[0].GetField<int>(0));
                Assert.AreEqual(1.1, result[0].GetField<double>(1));
                Assert.AreEqual("mystring", result[0].GetStringField(2));

                Assert.AreEqual(2, result[1].GetField<int>(0));
                Assert.AreEqual(2.2, result[1].GetField<double>(1));
                Assert.AreEqual("mystring2", result[1].GetStringField(2));

                await tran.Commit();
            }
        }

        [Test]
        public async Task E2EWithRollback()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, 'mystring')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (1, 1.1, 'mystring')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();

                insertQuery = "INSERT INTO Table VALUES (2, 2.2, 'mystring2')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Rollback();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (2, 2.2, 'mystring2')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM Table";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(1, result[0].GetField<int>(0));
                Assert.AreEqual(1.1, result[0].GetField<double>(1));
                Assert.AreEqual("mystring", result[0].GetStringField(2));

                Assert.AreEqual(2, result[1].GetField<int>(0));
                Assert.AreEqual(2.2, result[1].GetField<double>(1));
                Assert.AreEqual("mystring2", result[1].GetStringField(2));

                await tran.Commit();
            }
        }

        [Test]
        public void InsertInvalidName()
        {
            Assert.ThrowsAsync<KeyNotFoundException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = "INSERT INTO NOTEXISTINGTABLE VALUES (2, 2.2, 'mystring2')";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        public void ScanFromInvalidTable()
        {
            Assert.ThrowsAsync<KeyNotFoundException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string query = @"SELECT a, b, c FROM NOTEXISTINGTABLE";
                    await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }
    }

    public class E2EBufferPoolLimits
    {
        [Test]
        public async Task E2EBufferPoolExceed()
        {
            BufferPool bp = new BufferPool();

            IPageEvictionPolicy restrictiveEviction = new FifoEvictionPolicy(6, 1);
            ILockManager lm = new LockManager.LockManager(new LockMonitor(), TestGlobals.TestFileLogger);
            var stream = new PersistedStream(1024 * 1024, "bufferpoolexceed.data", createNew: true);
            var pageManager = new PageManager.PageManager(4096, restrictiveEviction, stream, bp, lm, TestGlobals.TestFileLogger);

            var logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            var metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager, stringHeap, pageManager);

            var queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });
            const int rowInsert = 1000;

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE LargeTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(20) c)";
                await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            for (int i = 0; i < rowInsert; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO LargeTable VALUES ({i}, {i}.1, 'mystring{i}')";
                    await queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "SELECT"))
            {
                string query = @"SELECT a, b, c FROM LargeTable";
                RowHolderFixed[] result = await queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(rowInsert, result.Length);
            }
        }
    }
}
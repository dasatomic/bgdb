using DataStructures;
using LockManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace E2EQueryExecutionTests
{
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
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager);

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

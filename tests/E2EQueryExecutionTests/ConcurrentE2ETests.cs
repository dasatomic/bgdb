using DataStructures;
using LockManager;
using LogManager;
using NUnit.Framework;
using PageManager;
using PageManager.Exceptions;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Test.Common;

namespace E2EQueryExecutionTests
{
    public class ConcurrentE2ETests
    {
        private QueryEntryGate queryEntryGate;
        private ILogManager logManager;
        private IPageManager pageManager;
        private MetadataManager.MetadataManager metadataManager;
        private Instrumentation.Logger logger;
        private ILockManager lockManager;

        [SetUp]
        public async Task Setup()
        {
            if (this.pageManager != null)
            {
                this.pageManager.Dispose();
            }

            this.lockManager = new LockManager.LockManager(new LockMonitor(), TestGlobals.TestFileLogger);

            if (this.logger == null)
            {
                this.logger = new Instrumentation.Logger("ConcurrencyTestLog.txt", "concurrency", Instrumentation.LogLevel.Info);
            }

            this.pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(100, 10), TestGlobals.DefaultPersistedStream, new BufferPool(), this.lockManager, logger);
            this.logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;
            StringHeapCollection metadataStringHeap = null;

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: false, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                metadataStringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            metadataManager = new MetadataManager.MetadataManager(pageManager, metadataStringHeap, pageManager, logManager);
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
        public async Task ConcurrentInserts()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager))
            {
                string createTableQuery = "CREATE TABLE ConcurrentTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            const int rowCount = 10;
            const int workerCount = 100;
            int totalSum = 0;
            int totalInsert = 0;

            async Task insertAction()
            {
                    for (int i = 1; i <= rowCount; i++)
                    {
                        using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                        {
                            string insertQuery = $"INSERT INTO ConcurrentTable VALUES ({i}, {i + 0.001}, 'mystring')";
                            await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                            await tran.Commit();
                            Interlocked.Add(ref totalSum, i);
                            Interlocked.Increment(ref totalInsert);
                        }
                }
            }

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < workerCount; i++)
            {
                tasks.Add(Task.Run(insertAction));
            }

            await Task.WhenAll(tasks);

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM ConcurrentTable";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(workerCount * rowCount, totalInsert);

                Assert.AreEqual(workerCount * rowCount, result.Length);

                int sum = result.Sum(r => r.GetField<int>(0));
                Assert.AreEqual(totalSum, sum);
                await tran.Commit();
            }

            LockStats lockStats = this.lockManager.GetLockStats();
            TestContext.Out.WriteLine(lockStats);
        }

        [Test]
        public async Task ConcurrentInsertsAndScan()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager))
            {
                string createTableQuery = "CREATE TABLE ConcurrentTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            const int rowCount = 50;
            const int writerCount = 50;
            const int readerCount = 10;
            int totalSum = 0;
            int totalInsert = 0;

            // TODO: Rollback shouldn't really happen in this case.
            async Task insertAction()
            {
                for (int i = 1; i <= rowCount; i++)
                {
                    bool success = false;
                    while (!success)
                    {
                        using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT_ROWS"))
                        {
                            try
                            {
                                string insertQuery = $"INSERT INTO ConcurrentTable VALUES ({i}, {i + 0.001}, 'mystring')";
                                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                                await tran.Commit();
                                Interlocked.Add(ref totalSum, i);
                                Interlocked.Increment(ref totalInsert);
                                success = true;
                            }
                            catch (DeadlockException)
                            {
                            }
                        }
                    }
                }
            }

            async Task selectAction()
            {
                using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "GET_ROWS"))
                {
                    try
                    {
                        string selectQuery = @"SELECT a, b, c FROM ConcurrentTable";
                        RowHolderFixed[] result = await this.queryEntryGate.Execute(selectQuery, tran).ToArrayAsync();
                    }
                    catch (DeadlockException)
                    {

                    }
                }
            }

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < writerCount; i++)
            {
                tasks.Add(Task.Run(insertAction));
            }

            for (int i = 0; i < readerCount; i++)
            {
                tasks.Add(Task.Run(selectAction));
            }

            await Task.WhenAll(tasks);

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM ConcurrentTable";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(writerCount* rowCount, totalInsert);

                Assert.AreEqual(writerCount * rowCount, result.Length);

                int sum = result.Sum(r => r.GetField<int>(0));
                Assert.AreEqual(totalSum, sum);
                await tran.Commit();
            }

            LockStats lockStats = this.lockManager.GetLockStats();
            TestContext.Out.WriteLine(lockStats);
        }
    }
}

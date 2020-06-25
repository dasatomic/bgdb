using DataStructures;
using LogManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
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

        [SetUp]
        public async Task Setup()
        {
            this.pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(100, 10), TestGlobals.DefaultPersistedStream);
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
        [Repeat(10)]
        public async Task ConcurrentInserts()
        {
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE ConcurrentTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            const int rowCount = 100;
            const int workerCount = 5;
            int totalSum = 0;
            int totalInsert = 0;

            Action insertAction = () =>
            {
                using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
                {
                    for (int i = 1; i <= rowCount; i++)
                    {
                        string insertQuery = $"INSERT INTO ConcurrentTable VALUES ({i}, {i + 0.001}, mystring)";
                        this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync().AsTask().Wait();
                        tran.Commit().Wait();
                        Interlocked.Add(ref totalSum, i);
                        Interlocked.Increment(ref totalInsert);
                        TestContext.Out.WriteLine("Done inserting {0}", i);
                    }
                }
            };

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < workerCount; i++)
            {
                tasks.Add(Task.Run(insertAction));
            }

            await Task.WhenAll(tasks);

            await using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM ConcurrentTable";
                Row[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(workerCount * rowCount, totalInsert);

                Assert.AreEqual(workerCount * rowCount, result.Length);

                int sum = result.Sum(r => r.IntCols[0]);
                Assert.AreEqual(totalSum, sum);
                await tran.Commit();
            }
        }
    }
}

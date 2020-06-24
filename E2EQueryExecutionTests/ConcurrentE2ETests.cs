using DataStructures;
using LogManager;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
        public async Task ConcurrentInserts()
        {
            await using (Transaction tran = new Transaction(logManager, pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE ConcurrentTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            Action<Tuple<int, double, string>> insertAction = async (Tuple<int, double, string> data) =>
            {
                await using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
                {
                    string insertQuery = "INSERT INTO ConcurrentTable VALUES (1, 1.1, mystring)";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            };

            IEnumerable<Tuple<int, double, string>> itemsToInsert = Enumerable.Range(0, 1000).Select(x => Tuple.Create(x, x * 1.1, x + ""));
            Parallel.ForEach(itemsToInsert, insertAction);

            await using (Transaction tran = new Transaction(logManager, pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM ConcurrentTable";
                Row[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                int sum = result.Sum(r => r.IntCols[0]);
                Assert.AreEqual(1000, sum);
                await tran.Commit();
            }
        }
    }
}

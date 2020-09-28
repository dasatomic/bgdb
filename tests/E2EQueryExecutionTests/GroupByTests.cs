using NuGet.Frameworks;
using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class GroupByTests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE MyTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            for (int i = 0; i < 100; i++)
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO MyTable VALUES ({i % 5}, {i + 0.1}, '{i}')";

                    // double insert.
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }

        [Test]
        public async Task GroupByE2E()
        {

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MAX(b)
FROM MyTable
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(5, result.Length);
                await tran.Commit();
            }
        }

        [Test]
        public async Task GroupByWithFilter()
        {

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MAX(b)
FROM MyTable
WHERE a < 4 
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(4, result.Length);
                await tran.Commit();
            }
        }

        [Test]
        public async Task AggCheckMax()
        {

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MAX(b)
FROM MyTable
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                for (int i = 0; i < 5; i++)
                {
                    Assert.AreEqual(i, result[i].GetField<int>(0));
                    Assert.AreEqual(i + 95.1, result[i].GetField<double>(1));
                }
            }
        }

        [Test]
        public async Task AggCheckMin()
        {

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MIN(b)
FROM MyTable
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                for (int i = 0; i < 5; i++)
                {
                    Assert.AreEqual(i, result[i].GetField<int>(0));
                    Assert.AreEqual(i + 0.1, result[i].GetField<double>(1));
                }
            }
        }

        [Test]
        public async Task AggNoGroupBy()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT MIN(b)
FROM MyTable
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(0.1, result[0].GetField<double>(0));
            }
        }

        [Test]
        public async Task GroupByNoAgg()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a
FROM MyTable
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(5, result.Length);
            }
        }

        [Test]
        [Ignore("Need to fix the bug.")]
        public async Task DoubleAggOnSameColumn()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MIN(b), MAX(b)
FROM MyTable
GROUP BY a
";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(5, result.Length);
            }
        }

        [Test]
        [Ignore("Need to fix the bug.")]
        public void GroupByInvalidColumn()
        {
            Assert.ThrowsAsync<KeyNotFoundException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"
SELECT a, MIN(b)
FROM MyTable
GROUP BY z
";
                    RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        [Ignore("Need to fix the bug.")]
        public void AggInvalidColumn()
        {
            Assert.ThrowsAsync<KeyNotFoundException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"
SELECT a, MIN(z)
FROM MyTable
GROUP BY a
";
                    RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }
    }
}

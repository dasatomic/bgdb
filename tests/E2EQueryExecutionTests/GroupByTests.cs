using NUnit.Framework;
using PageManager;
using QueryProcessing;
using QueryProcessing.Exceptions;
using System.Collections.Generic;
using System.Linq;
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
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
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(5, result.Length);
            }
        }

        [Test]
        public async Task DoubleAggOnSameColumn()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT a, MIN(b), MAX(b)
FROM MyTable
GROUP BY a
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(5, result.Length);
            }
        }

        [Test]
        public void GroupByInvalidColumn()
        {
            Assert.ThrowsAsync<InvalidColumnNameException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"
SELECT a, MIN(b)
FROM MyTable
GROUP BY z
";
                    RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        public void AggInvalidColumn()
        {
            Assert.ThrowsAsync<InvalidColumnNameException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"
SELECT a, MIN(z)
FROM MyTable
GROUP BY a
";
                    RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        public async Task SumTest()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT SUM(a), SUM(b), MAX(a)
FROM MyTable
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(400, result[0].GetField<int>(0));
                Assert.AreEqual(4, result[0].GetField<int>(2));
            }
        }

        [Test]
        public async Task CountTest()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT SUM(a), COUNT(b), COUNT(a), COUNT(c)
FROM MyTable
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(400, result[0].GetField<int>(0));
                Assert.AreEqual(200, result[0].GetField<int>(1));
                Assert.AreEqual(200, result[0].GetField<int>(2));
                Assert.AreEqual(200, result[0].GetField<int>(3));
            }
        }

        [Test]
        public async Task UsingFullNameCorrect()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT SUM(MyTable.a), COUNT(a), COUNT(MyTable.c), MyTable.b
FROM MyTable
GROUP BY b
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();
            }
        }

        [Test]
        public async Task UsingFullNameIncorrect()
        {
            Assert.ThrowsAsync<InvalidColumnNameException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"
    SELECT SUM(MyTable.a), COUNT(a), COUNT(MyTable.c), NotMyTable.b
    FROM MyTable
    GROUP BY b
    ";
                    RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        public async Task AggBeforeGroupBy()
        {

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT MAX(b), a, COUNT(b)
FROM MyTable
GROUP BY a
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                await tran.Commit();

                for (int i = 0; i < 5; i++)
                {
                    Assert.AreEqual(i + 95.1, result[i].GetField<double>(0));
                    Assert.AreEqual(i, result[i].GetField<int>(1));
                    Assert.AreEqual(40, result[i].GetField<int>(2));
                }
            }
        }

        [Test]
        public async Task AggAndGroupByEmptyTable()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager))
            {
                await this.queryEntryGate.Execute("CREATE TABLE EmptyTable (TYPE_INT A)", tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "SELECT"))
            {
                RowHolder[] result = await this.queryEntryGate.Execute("SELECT MAX(A) FROM EmptyTable", tran).ToArrayAsync();
                Assert.AreEqual(0, result.Length);
                result = await this.queryEntryGate.Execute("SELECT A FROM EmptyTable GROUP BY A", tran).ToArrayAsync();
                Assert.AreEqual(0, result.Length);
            }
        }

        [Test]
        public async Task MultiTableGroupBy()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager))
            {
                await this.queryEntryGate.Execute("CREATE TABLE T1 (TYPE_INT A, TYPE_INT B)", tran).AllResultsAsync();
                await this.queryEntryGate.Execute("CREATE TABLE T2 (TYPE_INT A, TYPE_INT B)", tran).AllResultsAsync();
                await this.queryEntryGate.Execute("INSERT INTO T1 VALUES (1, 1)", tran).AllResultsAsync();
                await this.queryEntryGate.Execute("INSERT INTO T1 VALUES (1, 2)", tran).AllResultsAsync();
                await this.queryEntryGate.Execute("INSERT INTO T2 VALUES (2, 3)", tran).AllResultsAsync();
                await this.queryEntryGate.Execute("INSERT INTO T2 VALUES (2, 4)", tran).AllResultsAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "SELECT"))
            {
                RowHolder[] result = await this.queryEntryGate.Execute("SELECT MAX(B), A FROM T1 GROUP BY A", tran).ToArrayAsync();
                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(2, result[0].GetField<int>(0));
                Assert.AreEqual(1, result[0].GetField<int>(1));
                result = await this.queryEntryGate.Execute("SELECT MAX(B), A FROM T2 GROUP BY A", tran).ToArrayAsync();
                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(4, result[0].GetField<int>(0));
                Assert.AreEqual(2, result[0].GetField<int>(1));
            }
        }
    }
}

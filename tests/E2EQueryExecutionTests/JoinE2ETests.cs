using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class JoinE2ETests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE T1 (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).AllResultsAsync();

                createTableQuery = "CREATE TABLE T2 (TYPE_INT a, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).AllResultsAsync();
                await tran.Commit();
            }

            for (int i = 0; i < 20; i++)
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO T1 VALUES ({i % 5}, {i + 0.1}, 'x{i % 5}T1')";
                    await this.queryEntryGate.Execute(insertQuery, tran).AllResultsAsync();

                    await tran.Commit();
                }
            }

            for (int i = 0; i < 10; i++)
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO T2 VALUES ({i % 5}, 'x{i % 5}T2')";
                    await this.queryEntryGate.Execute(insertQuery, tran).AllResultsAsync();

                    await tran.Commit();
                }
            }
        }

        [Test]
        public async Task InnerJoinCaseOne()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT *
FROM T1
JOIN T2 ON T1.a = T2.a
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                // T1.a is [0..4] X 5. T2.a is [0..4] X 2.
                // every T1.a has 2 matches in T2.a.
                // result count is T1 count X 2 (40).
                Assert.AreEqual(40, result.Length);

                for (int i = 0; i < result.Length; i++)
                {
                    int t1a = result[i].GetField<int>(0);
                    string t1c = new string(result[i].GetStringField(2));
                    int t2a = result[i].GetField<int>(3);
                    string t2c = new string(result[i].GetStringField(4));

                    Assert.AreEqual(t1a, t2a);
                    Assert.AreEqual($"x{t1a}T1", t1c);
                    Assert.AreEqual($"x{t2a}T2", t2c);
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task InnerJoinWithWhere()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT *
FROM T1
JOIN T2 ON T1.a = T2.a
WHERE T1.a = 1 OR T2.a = 2
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                // T1.a is [1] X 5. T2.a is [2] X 2.
                // every T1.a has 2 matches in T2.a.
                // result count is T2 count X 2 (40).
                // JOIN with ON will return and filter will run on:
                // 0 0 X 8 (no pass)
                // 1 1 X 8 (passing because of T1.a = 1)
                // 2 2 X 8 (passing because T2.a = 2)
                // 3 3 X 8 (no pass)
                // 4 4 X 8 (no pass)
                Assert.AreEqual(16, result.Length);

                for (int i = 0; i < result.Length; i++)
                {
                    int t1a = result[i].GetField<int>(0);
                    string t1c = new string(result[i].GetStringField(2));
                    int t2a = result[i].GetField<int>(3);
                    string t2c = new string(result[i].GetStringField(4));

                    Assert.AreEqual(t1a, t2a);
                    Assert.AreEqual($"x{t1a}T1", t1c);
                    Assert.AreEqual($"x{t2a}T2", t2c);
                    Assert.IsTrue(t1a == 1 || t2a == 2);
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task GroupbyJoinWhere()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"
SELECT SUM(T2.a), T1.c
FROM T1
JOIN T2 ON T1.a = T2.a
WHERE T1.a = 1 OR T2.a = 2
GROUP BY T1.c
";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                // T1.a is [1] X 5. T2.a is [2] X 2.
                // every T1.a has 2 matches in T2.a.
                // result count is T2 count X 2 (40).
                // JOIN with ON will return and filter will run on:
                // 0 0 X 8 (no pass)
                // 1 1 X 8 (passing because of T1.a = 1)
                // 2 2 X 8 (passing because T2.a = 2)
                // 3 3 X 8 (no pass)
                // 4 4 X 8 (no pass)
                //
                // Group by T2.a and SUM T2.b
                // groups are 1 and 2. Sum for 1 is 8, for 2 is 16.
                Assert.AreEqual(2, result.Length);

                Assert.AreEqual(8, result[0].GetField<int>(0));
                Assert.AreEqual("x1T1", result[0].GetStringField(1));

                Assert.AreEqual(16, result[1].GetField<int>(0));
                Assert.AreEqual("x2T1", result[1].GetStringField(1));

                await tran.Commit();
            }
        }

        [Test]
        public async Task JoinRegressionTest1()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE TR1 (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).AllResultsAsync();

                createTableQuery = "CREATE TABLE TR2 (TYPE_INT a, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).AllResultsAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string[] insertQuerys = new[]
                {
                    @"INSERT INTO TR1 VALUES (1, 1.1, 'somerandomstring1')",
                    @"INSERT INTO TR1 VALUES (2, 2.2, 'somerandomstring1')",
                    @"INSERT INTO TR1 VALUES (3, 2.2, 'somerandomstring2')",
                    @"INSERT INTO TR1 VALUES (5, 14.2, 'somerandomstring2')",
                    @"INSERT INTO TR1 VALUES (5, 14.2, 'somerandomstring2')",
                    @"INSERT INTO TR2 VALUES (1, 'somerandomstring2')",
                    @"INSERT INTO TR2 VALUES (100, 'somerandomstring2')",
                };

                foreach (string insertQuery in insertQuerys)
                {
                    await this.queryEntryGate.Execute(insertQuery, tran).AllResultsAsync();
                }

                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT * FROM TR1 JOIN TR2 ON TR1.c = TR2.c WHERE TR2.A = 100 AND TR1.a = 3";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                // Join returns 6 rows (3 (from  TR1) X 2 (from TR2)
                // filter removes one group from TR2 and one leaves one in TR1. We are left with 1 row.
                Assert.AreEqual(1, result.Length);

                Assert.AreEqual(3, result[0].GetField<int>(0));
                Assert.AreEqual("somerandomstring2", result[0].GetStringField(4));

                await tran.Commit();
            }
        }

    }
}

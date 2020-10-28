using NUnit.Framework;
using PageManager;
using QueryProcessing.Exceptions;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    class OrderByTests : BaseTestSetup
    {
        private class TestRow
        {
            public int a;
            public double b;
            public string c;
        };

        private TestRow[] testTable = new TestRow[] {
            new TestRow() { a = 3, b = 0.1, c = "row" },
            new TestRow() { a = 4, b = 0.2, c = "column" },
            new TestRow() { a = 2, b = 0.1, c = "lock" },
            new TestRow() { a = 0, b = 0.3, c = "lock" },
            new TestRow() { a = 1, b = 0.1, c = "pointer" }
        };

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

            for (int i = 0; i < testTable.Length; i++)
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = $"INSERT INTO MyTable VALUES ({testTable[i].a}, {testTable[i].b}, '{testTable[i].c}')";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }

        [Test]
        [TestCase(true, 0, 0, "SELECT a FROM MyTable ORDER BY a")]
        [TestCase(false, 2, 1, "SELECT c, a, b FROM MyTable ORDER BY b desc")]
        [TestCase(false, 1, 2, "SELECT a, c, b FROM MyTable ORDER BY c desc")]
        public async Task OrderByE2E(bool asc, int projectedColumnId, int columnId, string query)
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(5, result.Length);

                Compare(result, projectedColumnId, columnId, asc);

                await tran.Commit();
            }
        }

        [Test]
        public async Task OrderByTop()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT TOP 3 a FROM MyTable ORDER BY a DESC";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(3, result.Length);
                Assert.AreEqual(4, GetValue(result[0], 0, 0));
                CompareDesc(result, 0, 0);

                await tran.Commit();
            }
        }

        [Test]
        public async Task OrderByFilter()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT c FROM MyTable WHERE b = 0.1 ORDER BY a";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(3, result.Length);
                Assert.AreEqual(testTable[4].c, GetValue(result[0], 0, 2));
                Assert.AreEqual(testTable[2].c, GetValue(result[1], 0, 2));
                Assert.AreEqual(testTable[0].c, GetValue(result[2], 0, 2));
                await tran.Commit();
            }
        }

        [Test]
        public void OrderByInvalidColumn()
        {
            Assert.ThrowsAsync<InvalidColumnNameException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
                {
                    string query = @"SELECT a FROM MyTable WHERE b = 0.1 ORDER BY d";
                    RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        [Ignore("Design bug")]
        public async Task OrderByGroupBy()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT B FROM MyTable GROUP BY B ORDER BY B";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(3, result.Length);

                CompareAsc(result, 0, 1);

                await tran.Commit();
            }
        }

        #region Helper

        private static void Compare(RowHolder[] result, int projectedColumnId, int columnId, bool asc)
        {
            if (asc)
                CompareAsc(result, projectedColumnId, columnId);
            else
                CompareDesc(result, projectedColumnId, columnId);
        }

        private static void CompareDesc(RowHolder[] result, int projectedColumnId, int columnId)
        {
            for (int i = 0; i < result.Length - 1; ++i)
            {
                Assert.GreaterOrEqual(GetValue(result[i], projectedColumnId, columnId), GetValue(result[i + 1], projectedColumnId, columnId));
            }
        }

        private static void CompareAsc(RowHolder[] result, int projectedColumnId, int columnId)
        {
            for (int i = 0; i < result.Length - 1; ++i)
            {
                Assert.LessOrEqual(GetValue(result[i], projectedColumnId, columnId), GetValue(result[i + 1], projectedColumnId, columnId));
            }
        }

        private static IComparable GetValue(RowHolder row, int projectedColumnId, int columnId)
        {
            return columnId switch
            {
                0 => row.GetField<int>(projectedColumnId),
                1 => row.GetField<double>(projectedColumnId),
                2 => new string(row.GetStringField(projectedColumnId)),
                _ => throw new ArgumentException(),
            };
        }
        #endregion Helper
    }
}

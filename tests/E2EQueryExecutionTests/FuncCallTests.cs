using NUnit.Framework;
using PageManager;
using PageManager.Exceptions;
using QueryProcessing;
using QueryProcessing.Exceptions;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class FuncCallTests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE T1 (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                for (int i = 0; i < 20; i++)
                {
                    string insertQuery = $"INSERT INTO T1 VALUES ({i}, 1.1, 'mystring')";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddIntInt()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(a, a) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i * 2, row.GetField<int>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddIntDouble()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(a, b) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i + 1.1, row.GetField<double>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddDoubleInt()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(b, a) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i + 1.1, row.GetField<double>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddDoubleDouble()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(b, b) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(1.1 * 2.0, row.GetField<double>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddWithTop()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT TOP 5 ADD(a, a) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(5, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i * 2, row.GetField<int>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddWithScalars()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(11, 11) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                foreach (var row in result)
                {
                    Assert.AreEqual(22, row.GetField<int>(0));
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddIdAndScalar()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(a, 11) FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i + 11, row.GetField<int>(0));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task AddInvalidTypes()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(c, 11) FROM T1";
                Assert.ThrowsAsync<InvalidFunctionArgument>(async () => await this.queryEntryGate.Execute(query, tran).ToArrayAsync());
            }
        }

        [Test]
        public async Task AddAndColumn()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT ADD(a, 11), a FROM T1";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(20, result.Length);

                int i = 0;
                foreach (var row in result)
                {
                    Assert.AreEqual(i + 11, row.GetField<int>(0));
                    Assert.AreEqual(i, row.GetField<int>(1));
                    i++;
                }

                await tran.Commit();
            }
        }

        [Test]
        public async Task InvalidFunctionName()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT INVALIDFUNCTIONANME(11) FROM T1";
                Assert.ThrowsAsync<InvalidFunctionNameException>(async () => await this.queryEntryGate.Execute(query, tran).ToArrayAsync());
            }
        }
    }
}

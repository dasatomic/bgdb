using NUnit.Framework;
using PageManager;
using PageManager.Exceptions;
using QueryProcessing.Exceptions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class Tests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();
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

        [Test]
        public void SelectInvalidColumn()
        {
            Assert.ThrowsAsync<KeyNotFoundException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string createTableQuery = "CREATE TABLE TableInvalidColumn (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                    await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                    await tran.Commit();

                    string query = @"SELECT a, b, randomcolumnname FROM TableInvalidColumn";
                    await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                    await tran.Commit();
                }
            });
        }

        [Test]
        public async Task InsertCompatibleType()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_DOUBLE b)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                // value 1 will be parsed as INT, but it should be inserted as float.
                string insertQuery = "INSERT INTO Table VALUES (42)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "SELECT"))
            {
                string insertQuery = "SELECT b FROM Table";
                var result = await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();

                Assert.AreEqual(42, result[0].GetField<double>(0));
            }
        }

        [Test]
        public async Task InsertInCompatibleType()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT b)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            Assert.ThrowsAsync<InvalidColumnTypeException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    // Float will be truncated. Hence we don't allow it.
                    string insertQuery = "INSERT INTO Table VALUES (42.17)";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                }
            });
        }

        [Test]
        public async Task InsertWithReadOnlyTran()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT b)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            Assert.ThrowsAsync<ReadOnlyTranCantAcquireExLockException>(async () =>
            {
                await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "INSERT"))
                {
                    string insertQuery = "INSERT INTO Table VALUES (42)";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                }
            });
        }

        [Test]
        public async Task SelectWithReadonlyTran()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT b)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager,  "INSERT"))
            {
                string insertQuery = "INSERT INTO Table VALUES (42)";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "SELECT"))
            {
                string selectQuery = "SELECT b FROM Table";
                var res = await this.queryEntryGate.Execute(selectQuery, tran).ToArrayAsync();
                Assert.AreEqual(42, res[0].GetField<int>(0));
            }
        }
    }
}
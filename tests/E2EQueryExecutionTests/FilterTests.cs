using NUnit.Framework;
using PageManager;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class FilterTests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();
        }

        [Test]
        public async Task SimpleFilter()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE FilterTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO FilterTable VALUES (1, 1.1, 'mystring')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();

                insertQuery = "INSERT INTO FilterTable VALUES (2, 2.2, 'mystring2')";
                await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SELECT a, b, c FROM FilterTable WHERE a = 2";
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();

                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(2, result[0].GetField<int>(0));
                Assert.AreEqual(2.2, result[0].GetField<double>(1));
                Assert.AreEqual("mystring2", result[0].GetStringField(2));

                await tran.Commit();
            }
        }

        [Test]
        [TestCase(@"SELECT a, b, c FROM FilterTable WHERE a <= 50 AND b <= 50.0", 50)]
        [TestCase(@"SELECT a, b, c FROM FilterTable WHERE a <= 50 AND b > 51.0", 0)]
        [TestCase(@"SELECT a, b, c FROM FilterTable WHERE c = '99'", 1)]
        [TestCase(@"SELECT a, b, c FROM FilterTable WHERE c = '101'", 0)]
        public async Task ComplexFilter(string query, int expectedRowCount)
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE FilterTable (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                for (int i = 0; i < 100; i++)
                {
                    string insertQuery = $"INSERT INTO FilterTable VALUES ({i}, {i + 0.1}, '{i}')";
                    await this.queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                }

                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                RowHolderFixed[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(expectedRowCount, result.Length);
                await tran.Commit();
            }
        }
    }
}

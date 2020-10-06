using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class InsertTests : BaseTestSetup
    {

        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();
        }

        [Test]
        public async Task InsertSpecialCharsString()
        {
            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE SpecialCharsTable (TYPE_STRING(10) c)";
                await this.queryEntryGate.Execute(createTableQuery, tran).AllResultsAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "INSERT"))
            {
                string insertQuery = "INSERT INTO SpecialCharsTable VALUES ('my string ...')";
                await this.queryEntryGate.Execute(insertQuery, tran).AllResultsAsync();

                insertQuery = "INSERT INTO SpecialCharsTable VALUES ('... is here .')";
                await this.queryEntryGate.Execute(insertQuery, tran).AllResultsAsync();
                await tran.Commit();
            }

            await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS"))
            {
                string query = @"SeLEcT c FrOm SpecialCharsTable where c = 'my string ...'";
                RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual("my string ...", result[0].GetStringField(0));

                query = @"SeLEcT c FrOm SpecialCharsTable where c = 'my STring ...'";
                result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual(0, result.Length);

                query = @"SeLEcT c FrOm SpecialCharsTable where c = '... is here .'";
                result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
                Assert.AreEqual("... is here .", result[0].GetStringField(0));

                await tran.Commit();
            }
        }
    }
}

using ImageProcessing;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class ImageClassificationTests : BaseTestSetup
    {
        [SetUp]
        public new async Task Setup()
        {
            await base.Setup();
        }

        [Test]
        public async Task FetchFileSystemTest()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT CLASSIFY_IMAGE(FilePath), FilePath, FileName FROM FILESYSTEM('./assets/pics') WHERE EXTENSION = '.jpg' OR EXTENSION = '.jfif'";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new Dictionary<string, string>
            {
                { "basketball.jpg", "basketball" },
                { "hippo.jfif", "hippopotamus" },
            };

            foreach (RowHolder rh in result)
            {
                string classificationResult = new string(rh.GetStringField(0));
                string fileName = new string(rh.GetStringField(2));

                Assert.AreEqual(expectedResults[fileName], classificationResult);
            }
        }
    }
}

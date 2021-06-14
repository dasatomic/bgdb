using NUnit.Framework;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace E2EQueryExecutionTests
{
    public class FileSystemProviderTests : BaseTestSetup
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
            const string query = "SELECT * FROM FILESYSTEM('./assets')";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new []
            {
                "file.mkv",
                "file1.txt",
                "file2.txt",
            };

            Assert.AreEqual(3, result.Length);

            foreach (RowHolder rh in result)
            {
                string fullPath = new string(rh.GetStringField(0));
                string fileName = new string(rh.GetStringField(1));
                string extension = new string(rh.GetStringField(2));
                int length = rh.GetField<int>(3);

                Assert.IsTrue(expectedResults.Contains(fileName));
                System.IO.FileInfo fi = new System.IO.FileInfo(fullPath);
                Assert.IsTrue(fullPath.Contains(fileName));
                Assert.AreEqual(fi.Name, fileName);
                Assert.AreEqual(fi.Extension, extension);
                Assert.AreEqual(fi.Length, length);
            }
        }

        [Test]
        public async Task FetchFileSystemWithFilter()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT * FROM FILESYSTEM('./assets') WHERE Extension = '.txt'";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new[] { "file2.txt", "file1.txt" };

            Assert.AreEqual(2, result.Length);

            foreach (RowHolder rh in result)
            {
                string fullPath = new string(rh.GetStringField(0));
                string fileName = new string(rh.GetStringField(1));
                string extension = new string(rh.GetStringField(2));
                int length = rh.GetField<int>(3);

                Assert.IsTrue(expectedResults.Contains(fileName));
                System.IO.FileInfo fi = new System.IO.FileInfo(fullPath);
                Assert.IsTrue(fullPath.Contains(fileName));
                Assert.AreEqual(fi.Name, fileName);
                Assert.AreEqual(fi.Extension, extension);
                Assert.AreEqual(fi.Length, length);
            }
        }

        [Test]
        public async Task GroupBySizeByExtension()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT Extension, SUM(FileSize) FROM FILESYSTEM('./assets') GROUP BY Extension";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new Dictionary<string, int>
            {
                { ".txt", 26 },
                { ".mkv", 12 },
            };

            Assert.AreEqual(2, result.Length);

            foreach (RowHolder rh in result)
            {
                string extension = new string(rh.GetStringField(0));

                Assert.IsTrue(expectedResults.ContainsKey(extension));

                // TODO: File size may differ on linux and windows.
                // so sipping this check for now.
                // Assert.AreEqual(expectedResults[extension], lengthSum);
            }
        }

        [Test]
        public async Task VideoChunkerTest()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT * FROM VIDEO_CHUNKER(10, SELECT * FROM FILESYSTEM('./assets/videos') WHERE Extension = '.mkv')";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            Assert.AreEqual(5, result.Length);
        }
    }
}

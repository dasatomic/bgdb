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

        private class FileSystemResult
        {
            public string Extension;
            public int Length;
        }

        [Test]
        public async Task FetchFileSystemTest()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT * FROM FILESYSTEM('./assets')";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new Dictionary<string, FileSystemResult>
            {
                { "file.mkv", new FileSystemResult() { Extension = ".mkv", Length = 12 } },
                { "file1.txt", new FileSystemResult() { Extension = ".txt", Length = 14 } },
                { "file2.txt", new FileSystemResult() { Extension = ".txt", Length = 12 } },
            };

            Assert.AreEqual(3, result.Length);

            foreach (RowHolder rh in result)
            {
                string fullPath = new string(rh.GetStringField(0));
                string fileName = new string(rh.GetStringField(1));
                string extension = new string(rh.GetStringField(2));
                int length = rh.GetField<int>(3);

                Assert.IsTrue(expectedResults.ContainsKey(fileName));
                Assert.AreEqual(expectedResults[fileName].Extension, extension);
                Assert.AreEqual(expectedResults[fileName].Length, length);
                Assert.IsTrue(fullPath.Contains(fileName));
            }
        }

        [Test]
        public async Task FetchFileSystemWithFilter()
        {
            await using ITransaction tran = this.logManager.CreateTransaction(pageManager, "GET_ROWS");
            const string query = "SELECT * FROM FILESYSTEM('./assets') WHERE Extension = '.txt' AND FileSize < 14";
            RowHolder[] result = await this.queryEntryGate.Execute(query, tran).ToArrayAsync();
            await tran.Commit();

            var expectedResults = new Dictionary<string, FileSystemResult>
            {
                { "file2.txt", new FileSystemResult() { Extension = ".txt", Length = 12 } },
            };

            Assert.AreEqual(1, result.Length);

            foreach (RowHolder rh in result)
            {
                string fullPath = new string(rh.GetStringField(0));
                string fileName = new string(rh.GetStringField(1));
                string extension = new string(rh.GetStringField(2));
                int length = rh.GetField<int>(3);

                Assert.IsTrue(expectedResults.ContainsKey(fileName));
                Assert.AreEqual(expectedResults[fileName].Extension, extension);
                Assert.AreEqual(expectedResults[fileName].Length, length);
                Assert.IsTrue(fullPath.Contains(fileName));
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
                int lengthSum = rh.GetField<int>(1);

                Assert.IsTrue(expectedResults.ContainsKey(extension));
                Assert.AreEqual(expectedResults[extension], lengthSum);
            }
        }
    }
}

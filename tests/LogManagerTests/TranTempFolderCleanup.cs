using LogManager;
using NUnit.Framework;
using PageManager;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace LogManagerTests
{
    public class TranTempFolderCleanup
    {
        private ILogManager logManager;
        private IPageManager pageManager;

        [SetUp]
        public void Setup()
        {
            Stream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);
            pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            logManager = new LogManager.LogManager(writer);
        }

        public enum TranAction
        {
            Commit,
            Rollback,
        }

        [Test, Pairwise]
        public async Task FolderCleanupCheck(
            [Values(true, false)] bool isReadonly,
            [Values(TranAction.Commit, TranAction.Rollback)] TranAction action)
        {
            await using ITransaction tran1 = logManager.CreateTransaction(pageManager, isReadonly, "folder_check");

            string tempDirName = Guid.NewGuid().ToString();
            DirectoryInfo tempDirInfo = Directory.CreateDirectory(tempDirName);
            string currentDir = Directory.GetCurrentDirectory();

            string[] dirs = Directory.GetDirectories(currentDir);
            Assert.Contains(tempDirInfo.FullName, dirs);

            tran1.RegisterTempFolder(tempDirInfo);

            if (!isReadonly)
            {
                if (action == TranAction.Commit)
                {
                    await tran1.Commit();
                }
                else if (action == TranAction.Rollback)
                {
                    await tran1.Rollback();
                }
            }

            await tran1.DisposeAsync();

            dirs = Directory.GetDirectories(currentDir);
            Assert.IsFalse(dirs.Contains(tempDirInfo.FullName));
        }
    }
}

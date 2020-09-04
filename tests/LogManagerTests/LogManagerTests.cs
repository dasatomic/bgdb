using NUnit.Framework;
using LogManager;
using System.IO;
using System.Threading.Tasks;
using PageManager;
using System;
using System.Text;
using System.Linq;
using Test.Common;
using LockManager;
using System.Collections.Generic;

namespace LogManagerTests
{
    public class LogManagerTests
    {
        [Test]
        public async Task TranCommitSimple()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);

                ILogRecord record1 =
                    new UpdateRowRecord(
                        pageId: 1,
                        rowPosition: 2,
                        diffOldValue: new byte[] { 1, 2, 3 },
                        diffNewValue: new byte[] { 3, 2, 1 },
                        transactionId: tran1.TranscationId(),
                        new ColumnType[1] { ColumnType.Int },
                        PageType.IntPage);

                tran1.AddRecord(record1);
                await tran1.Commit();

                // Read the stream from beginning
                stream.Position = 0;

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());

                using (BinaryReader br = new BinaryReader(stream))
                {
                    LogRecordType lrType = (LogRecordType)br.ReadByte();
                    Assert.AreEqual(LogRecordType.RowModify, lrType);

                    UpdateRowRecord recordFromLog = new UpdateRowRecord(br);
                    Assert.AreEqual(LogRecordType.RowModify, recordFromLog.GetRecordType());
                    Assert.AreEqual(tran1.TranscationId(), recordFromLog.TransactionId());
                    Assert.AreEqual(2, recordFromLog.RowPosition);
                    Assert.AreEqual(new byte[] { 1, 2, 3 }, recordFromLog.DiffOldValue);
                    Assert.AreEqual(new byte[] { 3, 2, 1 }, recordFromLog.DiffNewValue);

                    lrType = (LogRecordType)br.ReadByte();
                    Assert.AreEqual(LogRecordType.Commit, lrType);
                    ulong tranId = br.ReadUInt64();
                    Assert.AreEqual(tran1.TranscationId(), tranId);
                }
            }
        }

        private async Task RollbackTest1<T>(
            Func<IPageManager, ITransaction, IPageSerializer<IEnumerable<T>, T>> pageCreate)
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);
                var page = pageCreate(pageManager, tran1);

                await tran1.Rollback();
                Assert.AreEqual(TransactionState.RollBacked, tran1.GetTransactionState());

                await using ITransaction tran2 = manager.CreateTransaction(pageManager);
                using var lck = await tran2.AcquireLock(page.PageId(), LockTypeEnum.Shared);
                T[] pageContent = page.Fetch(tran2).ToArray();
                Assert.AreEqual(0, pageContent.Length);
            }
        }

        private async Task RollbackTest2<T>(
            Func<IPageManager, ITransaction, IPageSerializer<IEnumerable<T>, T>> pageCreate,
            Action<IPageSerializer<IEnumerable<T>, T>, ITransaction> pageModify,
            Action<T[]> verify)
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);

                var page = pageCreate(pageManager, tran1);

                await tran1.Commit();

                await using ITransaction tran2 = manager.CreateTransaction(pageManager);
                pageModify(page, tran2);

                await tran2.Rollback();

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.RollBacked, tran2.GetTransactionState());

                await using ITransaction tran3 = manager.CreateTransaction(pageManager);
                using var lck = await tran3.AcquireLock(page.PageId(), LockTypeEnum.Shared);
                T[] pageContent = page.Fetch(tran3).ToArray();
                verify(pageContent);
            }
        }

        [Test]
        public async Task RollbackStrPage()
        {
            await RollbackTest1<char[]>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
                    using var rs = tran.AcquireLock(page.PageId(), LockManager.LockTypeEnum.Exclusive).Result;
                    page.Insert("TST".ToCharArray(), tran);
                    page.Insert("TST".ToCharArray(), tran);
                    page.Insert("TST".ToCharArray(), tran);
                    return page;
                });
        }

        [Test]
        public async Task RollbackStrPage2()
        {
            var pageContent = new List<char[]>{ "TST".ToCharArray(), "TST".ToCharArray(), "TST".ToCharArray() };
            await RollbackTest2<char[]>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
                    using var rs = tran.AcquireLock(page.PageId(), LockManager.LockTypeEnum.Exclusive).Result;
                    pageContent.ForEach(r => page.Insert(r, tran));
                    return page;
                },
                (p, tran) =>
                {
                    using var rs = tran.AcquireLock(p.PageId(), LockManager.LockTypeEnum.Exclusive).Result;
                    pageContent.ForEach(r => p.Insert(r, tran));
                },
                (i) => Assert.AreEqual(pageContent, i));
        }

        [Test]
        public async Task RollbackMixedPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);

                var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] columnType);
                MixedPage page = await pageManager.AllocateMixedPage(columnType, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);
                {
                    using var rs = await tran1.AcquireLock(page.PageId(), LockTypeEnum.Exclusive);
                    rows.ForEach(r => page.Insert(r, tran1));
                }

                await tran1.Commit();

                await using ITransaction tran2 = manager.CreateTransaction(pageManager);
                {
                    using var rs = await tran2.AcquireLock(page.PageId(), LockTypeEnum.Exclusive);
                    rows.ForEach(r => page.Insert(r, tran2));
                }

                await tran2.Rollback();

                await using ITransaction tran3 = manager.CreateTransaction(pageManager);
                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.RollBacked, tran2.GetTransactionState());

                using var lck = await tran3.AcquireLock(page.PageId(), LockTypeEnum.Shared);
                RowsetHolderFixed pageContent = page.Fetch(tran3);

                Assert.AreEqual(rows, pageContent.Iterate(columnType));
            }
        }

        [Test]
        public async Task RedoMixedPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);

                var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] columnTypes);
                MixedPage page = await pageManager.AllocateMixedPage(columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);

                {
                    using var rs = await tran1.AcquireLock(page.PageId(), LockTypeEnum.Exclusive);
                    rows.ForEach(r => page.Insert(r, tran1));
                }

                await tran1.Commit();

                await using ITransaction tran2 = manager.CreateTransaction(pageManager);

                {
                    using var rs = await tran2.AcquireLock(page.PageId(), LockTypeEnum.Exclusive);
                    rows.ForEach(r => page.Insert(r, tran2));
                }

                await tran2.Commit();

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.Committed, tran2.GetTransactionState());

                await using ITransaction tran3 = manager.CreateTransaction(pageManager);
                using var lck = await tran3.AcquireLock(page.PageId(), LockTypeEnum.Shared);
                var rowsPriorToRollback = page.Fetch(tran3);

                stream.Seek(0, SeekOrigin.Begin);
                pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                Assert.AreEqual(0, pageManager.PageCount());

                using (BinaryReader br = new BinaryReader(stream))
                {
                    await using ITransaction recTran = new DummyTran();
                    await manager.Recovery(br, pageManager, recTran);
                }

                var np1 = pageManager.GetMixedPage(page.PageId(), new DummyTran(), columnTypes);
                RowsetHolderFixed pageContent = page.Fetch(tran3);
                Assert.AreEqual(rowsPriorToRollback, pageContent);
            }
        }

        [Test]
        public async Task PageAllocRedo()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                await using ITransaction tran1 = manager.CreateTransaction(pageManager);

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                const int pageCount = 2;

                var p1 = await pageManager.AllocateMixedPage(types1, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);
                var p2 = await pageManager.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);

                await tran1.Commit();

                // Restart page manager.
                stream.Seek(0, SeekOrigin.Begin);
                pageManager =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                Assert.AreEqual(0, pageManager.PageCount());

                using (BinaryReader br = new BinaryReader(stream))
                {
                    await using ITransaction recTran = new DummyTran();
                    await manager.Recovery(br, pageManager, recTran);
                }

                Assert.AreEqual(pageCount, pageManager.PageCount());

                var np1 = await pageManager.GetMixedPage(p1.PageId(), new DummyTran(), types1);
                var np2 = await pageManager.GetPageStr(p2.PageId(), new DummyTran());

                Assert.IsTrue(p1.Equals(np1, TestGlobals.DummyTran));
                Assert.IsTrue(p2.Equals(np2, TestGlobals.DummyTran));
            }
        }
    }
}
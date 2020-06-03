using NUnit.Framework;
using LogManager;
using System.IO;
using System.Threading.Tasks;
using PageManager;
using System;
using System.Text;
using System.Linq;
using Test.Common;

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
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                ILogRecord record1 =
                    new UpdateRowRecord(
                        pageId: 1,
                        rowPosition: 2,
                        diffOldValue: new byte[] { 1, 2, 3 },
                        diffNewValue: new byte[] { 3, 2, 1 },
                        transactionId: tran1.TranscationId());

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
            Func<IPageManager, ITransaction, PageSerializerBase<T[]>> pageCreate)
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");
                var page = pageCreate(pageManager, tran1);

                await tran1.Rollback();
                Assert.AreEqual(TransactionState.RollBacked, tran1.GetTransactionState());
                T[] pageContent = page.Fetch();
                Assert.AreEqual(0, pageContent.Length);
            }
        }

        private async Task RollbackTest2<T>(
            Func<IPageManager, ITransaction, PageSerializerBase<T[]>> pageCreate,
            Action<PageSerializerBase<T[]>, ITransaction> pageModify,
            Action<T[]> verify)
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                var page = pageCreate(pageManager, tran1);

                await tran1.Commit();

                using ITransaction tran2 = new Transaction(manager, pageManager, "TRAN_TEST");
                pageModify(page, tran2);

                await tran2.Rollback();

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.RollBacked, tran2.GetTransactionState());

                T[] pageContent = page.Fetch();
                verify(pageContent);
            }
        }

        [Test]
        public async Task RollbackIntPage()
        {
            await RollbackTest1<int>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageInt(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new[] { 3, 2, 1 }, tran);
                    return page;
                });
        }

        [Test]
        public async Task RollbackLongPage()
        {
            await RollbackTest1<long>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageLong(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new long [] { 3, 2, 1 }, tran);
                    return page;
                });
        }

        [Test]
        public async Task RollbackDoublePage()
        {
            await RollbackTest1<double>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageDouble(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new double[] { 3, 2, 1 }, tran);
                    return page;
                });
        }

        [Test]
        public async Task RollbackStrPage()
        {
            await RollbackTest1<char[]>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new char[][] { "TST".ToCharArray(), "TST".ToCharArray(), "TST".ToCharArray() }, tran);
                    return page;
                });
        }

        [Test]
        public async Task RollbackIntPage2()
        {
            await RollbackTest2<int>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageInt(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new[] { 3, 2, 1 }, tran);
                    return page;
                },
                (p, tran) =>
                {
                    p.Merge(new[] { 3, 2, 1 }, tran);
                },
                (i) => Assert.AreEqual(new int[] { 3, 2, 1 }, i));
        }

        [Test]
        public async Task RollbackLongPage2()
        {
            await RollbackTest2<long>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageLong(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new long[] { 3, 2, 1 }, tran);
                    return page;
                },
                (p, tran) =>
                {
                    p.Merge(new long[] { 3, 2, 1 }, tran);
                },
                (i) => Assert.AreEqual(new long[] { 3, 2, 1 }, i));
        }

        [Test]
        public async Task RollbackDoublePage2()
        {
            await RollbackTest2<double>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageDouble(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(new double[] { 3, 2, 1 }, tran);
                    return page;
                },
                (p, tran) =>
                {
                    p.Merge(new double[] { 3, 2, 1 }, tran);
                },
                (i) => Assert.AreEqual(new double[] { 3, 2, 1 }, i));
        }

        [Test]
        public async Task RollbackStrPage2()
        {
            var pageContent = new char[][] { "TST".ToCharArray(), "TST".ToCharArray(), "TST".ToCharArray() };
            await RollbackTest2<char[]>(
                (pm, tran) =>
                {
                    var page = pm.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    page.Merge(pageContent, tran);
                    return page;
                },
                (p, tran) => p.Merge(pageContent, tran),
                (i) => Assert.AreEqual(pageContent, i));
        }

        [Test]
        public async Task RollbackMixedPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                MixedPage page = pageManager.AllocateMixedPage(types1, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);

                RowsetHolder holder = new RowsetHolder(types1);
                holder.SetColumns(intColumns1, doubleColumns1, pagePointerOffsetColumns1, pagePointerColumns1);
                page.Merge(holder, tran1);
                await tran1.Commit();

                using ITransaction tran2 = new Transaction(manager, pageManager, "TRAN_TEST");
                GenerateDataUtils.GenerateSampleData(out ColumnType[] types2, out int[][] intColumns2, out double[][] doubleColumns2, out long[][] pagePointerColumns2, out PagePointerOffsetPair[][] pagePointerOffsetColumns2, 1);
                RowsetHolder updateRow = new RowsetHolder(types2);
                updateRow.SetColumns(intColumns2, doubleColumns2, pagePointerOffsetColumns2, pagePointerColumns2);
                page.Merge(updateRow, tran2);

                await tran2.Rollback();

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.RollBacked, tran2.GetTransactionState());
                RowsetHolder pageContent = page.Fetch();

                Assert.AreEqual(holder, pageContent);
            }
        }

        [Test]
        public async Task RedoMixedPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                MixedPage page = pageManager.AllocateMixedPage(types1, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);

                RowsetHolder holder = new RowsetHolder(types1);
                holder.SetColumns(intColumns1, doubleColumns1, pagePointerOffsetColumns1, pagePointerColumns1);
                page.Merge(holder, tran1);
                await tran1.Commit();

                using ITransaction tran2 = new Transaction(manager, pageManager, "TRAN_TEST");
                GenerateDataUtils.GenerateSampleData(out ColumnType[] types2, out int[][] intColumns2, out double[][] doubleColumns2, out long[][] pagePointerColumns2, out PagePointerOffsetPair[][] pagePointerOffsetColumns2, 1);
                RowsetHolder updateRow = new RowsetHolder(types2);
                updateRow.SetColumns(intColumns2, doubleColumns2, pagePointerOffsetColumns2, pagePointerColumns2);
                page.Merge(updateRow, tran2);

                await tran2.Commit();

                Assert.AreEqual(TransactionState.Committed, tran1.GetTransactionState());
                Assert.AreEqual(TransactionState.Committed, tran2.GetTransactionState());

                holder = page.Fetch();

                stream.Seek(0, SeekOrigin.Begin);
                pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                Assert.AreEqual(0, pageManager.PageCount());

                using (BinaryReader br = new BinaryReader(stream))
                {
                    using ITransaction recTran = new DummyTran();
                    await manager.Recovery(br, pageManager, recTran);
                }

                var np1 = pageManager.GetMixedPage(page.PageId(), new DummyTran());
                RowsetHolder pageContent = page.Fetch();
                Assert.AreEqual(holder, pageContent);
            }
        }

        [Test]
        public async Task PageAllocRedo()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                const int pageCount = 3;

                var p1 = pageManager.AllocateMixedPage(types1, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran1);
                var p2 = pageManager.AllocatePageDouble(2, 2, tran1);
                var p3 = pageManager.AllocatePageInt(3, 3, tran1);

                await tran1.Commit();

                // Restart page manager.
                stream.Seek(0, SeekOrigin.Begin);
                pageManager =  new InMemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
                Assert.AreEqual(0, pageManager.PageCount());

                using (BinaryReader br = new BinaryReader(stream))
                {
                    using ITransaction recTran = new DummyTran();
                    await manager.Recovery(br, pageManager, recTran);
                }

                Assert.AreEqual(pageCount, pageManager.PageCount());

                var np1 = pageManager.GetMixedPage(p1.PageId(), new DummyTran());
                var np2 = pageManager.GetPageDouble(p2.PageId(), new DummyTran());
                var np3 = pageManager.GetPageInt(p3.PageId(), new DummyTran());

                Assert.AreEqual(p1, np1);
                Assert.AreEqual(p2, np2);
                Assert.AreEqual(p3, np3);
            }
        }
    }
}
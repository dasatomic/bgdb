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
                IPageManager pageManager = new InMemoryPageManager(4096);
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

        [Test]
        public async Task RollbackIntPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager = new InMemoryPageManager(4096);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                var page = pageManager.AllocatePageInt(0, 0, tran1);
                page.Merge(new[] { 3, 2, 1 }, tran1);

                ILogRecord record1 =
                    new UpdateRowRecord(
                        pageId: page.PageId(),
                        rowPosition: 2,
                        diffOldValue: BitConverter.GetBytes(42),
                        diffNewValue: BitConverter.GetBytes(43),
                        transactionId: tran1.TranscationId());

                tran1.AddRecord(record1);
                await tran1.Rollback();

                Assert.AreEqual(TransactionState.RollBacked, tran1.GetTransactionState());
                int[] pageContent = page.Fetch();
                Assert.AreEqual(42, pageContent[2]);
            }
        }

        [Test]
        public async Task RollbackStringPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager = new InMemoryPageManager(4096);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                var page = pageManager.AllocatePageStr(0, 0, tran1);
                page.Merge(new[] { "TEST1".ToCharArray(), "TEST2".ToCharArray(), "TEST3".ToCharArray() }, tran1);

                ILogRecord record1 =
                    new UpdateRowRecord(
                        pageId: page.PageId(),
                        rowPosition: 2,
                        diffOldValue: BitConverter.GetBytes((short)9).Concat(Encoding.ASCII.GetBytes("OLD VALUE")).ToArray(),
                        diffNewValue: BitConverter.GetBytes((short)9).Concat(Encoding.ASCII.GetBytes("NEW VALUE")).ToArray(),
                        transactionId: tran1.TranscationId());

                tran1.AddRecord(record1);
                await tran1.Rollback();

                Assert.AreEqual(TransactionState.RollBacked, tran1.GetTransactionState());
                char[][] pageContent = page.Fetch();
                Assert.AreEqual("OLD VALUE", new string(pageContent[2]));
            }
        }

        [Test]
        public async Task RollbackMixedPage()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager = new InMemoryPageManager(4096);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                MixedPage page = pageManager.AllocateMixedPage(types1, 0, 0, tran1);

                RowsetHolder holder = new RowsetHolder(types1);
                holder.SetColumns(intColumns1, doubleColumns1, pagePointerOffsetColumns1, pagePointerColumns1);

                page.Merge(holder, tran1);

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types2, out int[][] intColumns2, out double[][] doubleColumns2, out long[][] pagePointerColumns2, out PagePointerOffsetPair[][] pagePointerOffsetColumns2, 1);
                RowsetHolder updateRow = new RowsetHolder(types2);
                updateRow.SetColumns(intColumns2, doubleColumns2, pagePointerOffsetColumns2, pagePointerColumns2);

                byte[] serializedRow = new byte[updateRow.StorageSizeInBytes()];
                using (MemoryStream ms = new MemoryStream(serializedRow))
                using (BinaryWriter bw = new BinaryWriter(ms))
                {
                    updateRow.Serialize(bw);
                }

                ILogRecord record1 =
                    new UpdateRowRecord(
                        pageId: page.PageId(),
                        rowPosition: 2,
                        diffOldValue: serializedRow,
                        diffNewValue: serializedRow,
                        transactionId: tran1.TranscationId());

                tran1.AddRecord(record1);
                await tran1.Rollback();

                Assert.AreEqual(TransactionState.RollBacked, tran1.GetTransactionState());
                RowsetHolder pageContent = page.Fetch();

                Assert.AreEqual(pageContent.GetIntColumn(0)[2], updateRow.GetIntColumn(0)[0]);
                Assert.AreEqual(pageContent.GetIntColumn(1)[2], updateRow.GetIntColumn(1)[0]);
                Assert.AreEqual(pageContent.GetDoubleColumn(2)[2], updateRow.GetDoubleColumn(2)[0]);
                Assert.AreEqual(pageContent.GetIntColumn(3)[2], updateRow.GetIntColumn(3)[0]);
            }
        }

        [Test]
        public async Task PageAllocRedo()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                IPageManager pageManager = new InMemoryPageManager(4096);
                ILogManager manager = new LogManager.LogManager(writer);

                using ITransaction tran1 = new Transaction(manager, pageManager, "TRAN_TEST");

                GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
                const int pageCount = 3;

                var p1 = pageManager.AllocateMixedPage(types1, 0, 0, tran1);
                var p2 = pageManager.AllocatePageDouble(2, 2, tran1);
                var p3 = pageManager.AllocatePageInt(3, 3, tran1);

                await tran1.Commit();

                // Restart page manager.
                stream.Seek(0, SeekOrigin.Begin);
                pageManager = new InMemoryPageManager(4096);
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
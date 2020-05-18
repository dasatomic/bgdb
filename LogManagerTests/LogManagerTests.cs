using NUnit.Framework;
using LogManager;
using System.IO;
using System.Threading.Tasks;
using PageManager;

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
                    new PageModifyRecord(
                        pageId: 1,
                        pageOffsetDiffStart: 2,
                        diffOldValue: new byte[] { 1, 2, 3 },
                        diffNewValue: new byte[] { 3, 2, 1 },
                        transactionId: tran1.TranscationId());

                tran1.AddRecord(record1);
                await tran1.Commit();

                // Read the stream from beginning
                stream.Position = 0;

                using (BinaryReader br = new BinaryReader(stream))
                {
                    LogRecordType lrType = (LogRecordType)br.ReadByte();
                    Assert.AreEqual(LogRecordType.PageModify, lrType);

                    PageModifyRecord recordFromLog = new PageModifyRecord(br);
                    Assert.AreEqual(LogRecordType.PageModify, recordFromLog.GetRecordType());
                    Assert.AreEqual(tran1.TranscationId(), recordFromLog.TransactionId());
                    Assert.AreEqual(2, recordFromLog.PageOffsetDiffStart);
                    Assert.AreEqual(new byte[] { 1, 2, 3 }, recordFromLog.DiffOldValue);
                    Assert.AreEqual(new byte[] { 3, 2, 1 }, recordFromLog.DiffNewValue);
                }
            }
        }
    }
}
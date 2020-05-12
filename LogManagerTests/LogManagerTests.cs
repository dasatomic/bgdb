using NUnit.Framework;
using LogManager;
using System.IO;

namespace LogManagerTests
{
    public class LogManagerTests
    {
        [Test]
        public void Test1()
        {
            using (Stream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                ILogManager manager = new LogManager.LogManager(writer);

                ITransaction tran1 = new Transaction(manager);

                ILogRecord record1 =
                    new PageModifyRecord(
                        pageId: 1,
                        pageOffsetDiffStart: 2,
                        diffOldValue: new byte[] { 1, 2, 3 },
                        diffNewValue: new byte[] { 3, 2, 1 },
                        transactionId: tran1.TranscationId());

                tran1.AddRecord(record1);
                tran1.Commit();

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
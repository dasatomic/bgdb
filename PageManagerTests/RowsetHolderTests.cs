using NUnit.Framework;
using PageManager;
using System.IO;
using Test.Common;

namespace PageManagerTests
{
    class RowsetHolderTests
    {
        [Test]
        public void VerifyDataCorrectness()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            Assert.AreEqual(holder.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(3), intColumns[2]);
            Assert.AreEqual(holder.GetStringPointerColumn(4), pagePointerOffsetColumns[0]);
            Assert.AreEqual(holder.GetPagePointerColumn(5), pagePointerColumns[0]);

            Assert.AreEqual(5, holder.GetRowCount());
        }

        [Test]
        public void VerifyInvalidColumnSet()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, new double[0][], new PagePointerOffsetPair[0][], new long[0][]));

            intColumns[0] = new int[1] { 1 };
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, doubleColumns, new PagePointerOffsetPair[0][], new long[0][]));
        }

        [Test]
        public void VerifySerialization()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            byte[] content = new byte[holder.StorageSizeInBytes()];

            using (BinaryWriter bw = new BinaryWriter(new MemoryStream(content)))
            {
                holder.Serialize(bw);
            }

            IRowsetHolder holder2 = new RowsetHolder(types);
            holder2.Deserialize(new BinaryReader(new MemoryStream(content)), holder.GetRowCount());

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
            Assert.AreEqual(holder.GetStringPointerColumn(4), pagePointerOffsetColumns[0]);
            Assert.AreEqual(holder.GetPagePointerColumn(5), pagePointerColumns[0]);
        }

        [Test]
        public void EmptyRowset()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            IRowsetHolder holder = new RowsetHolder(types);

            Assert.IsEmpty(holder.GetIntColumn(0));
        }
    }
}

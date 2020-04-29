using NUnit.Framework;
using PageManager;

namespace PageManagerTests
{
    class RowsetHolderTests
    {
        [Test]
        public void VerifyDataCorrectness()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out PagePointerPair[][] pagePointerColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerColumns);

            Assert.AreEqual(holder.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(3), intColumns[2]);
            Assert.AreEqual(holder.GetStringPointerColumn(4), pagePointerColumns[0]);

            Assert.AreEqual(5, holder.GetRowCount());
        }

        [Test]
        public void VerifyInvalidColumnSet()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out PagePointerPair[][] pagePointerColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, new double[0][], new PagePointerPair[0][]));

            intColumns[0] = new int[1] { 1 };
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, doubleColumns, new PagePointerPair[0][]));
        }

        [Test]
        public void VerifySerialization()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out PagePointerPair[][] pagePointerColumns);
            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerColumns);

            byte[] content = holder.Serialize();

            IRowsetHolder holder2 = new RowsetHolder(types);
            holder2.Deserialize(content);

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
            Assert.AreEqual(holder.GetStringPointerColumn(4), pagePointerColumns[0]);
        }
    }
}

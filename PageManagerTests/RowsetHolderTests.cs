using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PageManagerTests
{
    class RowsetHolderTests
    {
        private void GenerateSampleData(
            out ColumnType[] types,
            out int[][] intColumns,
            out double[][] doubleColumns)
        {
            types = new ColumnType[]
            {
                ColumnType.Int,
                ColumnType.Int,
                ColumnType.Double,
                ColumnType.Int,
            };

            int intColumnCount = types.Count(t => t == ColumnType.Int);
            int doubleColumnCount = types.Count(t => t == ColumnType.Double);

            const int rowCount = 5;

            intColumns = new int[intColumnCount][];
            for (int i = 0; i < intColumns.Length; i++)
            {
                intColumns[i] = Enumerable.Repeat(i, rowCount).ToArray();
            }

            doubleColumns = new double[doubleColumnCount][];
            for (int i = 0; i < doubleColumns.Length; i++)
            {
                doubleColumns[i] = new double[rowCount];

                for (int j = 0; j < rowCount; j++)
                {
                    doubleColumns[i][j] = (double)j;
                }
            }
        }

        [Test]
        public void VerifyDataCorrectness()
        {
            GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, new PagePointerPair[0][]);

            Assert.AreEqual(holder.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder.GetIntColumn(3), intColumns[2]);

            Assert.AreEqual(5, holder.GetRowCount());
        }

        [Test]
        public void VerifyInvalidColumnSet()
        {
            GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns);

            IRowsetHolder holder = new RowsetHolder(types);
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, new double[0][], new PagePointerPair[0][]));

            intColumns[0] = new int[1] { 1 };
            Assert.Throws<InvalidRowsetDefinitionException>(() => holder.SetColumns(intColumns, doubleColumns, new PagePointerPair[0][]));
        }

        [Test]
        public void VerifySerialization()
        {
            GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns);
            IRowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, new PagePointerPair[0][]);

            byte[] content = holder.Serialize();

            IRowsetHolder holder2 = new RowsetHolder(types);
            holder2.Deserialize(content);

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
        }
    }
}

using NUnit.Framework;
using PageManager;
using QueryProcessing;

namespace QueryProcessingTests
{
    public class RowManipluationTests
    {
        [Test]
        public void TestProject()
        {
            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double, ColumnType.Int };
            Row source = new Row(new[] { 1, 2 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes);
            Row dest = source.Project(new int[] { 1 });

            Assert.IsTrue(dest.DoubleCols.Length == 0);
            Assert.IsTrue(dest.IntCols.Length == 0);
            Assert.IsTrue(dest.StringCols.Length == 1);
            Assert.AreEqual(new ColumnType[] { ColumnType.StringPointer }, dest.ColumnTypesOrdered);

            dest = source.Project(new int[] { 0, 1 });

            Assert.IsTrue(dest.DoubleCols.Length == 0);
            Assert.IsTrue(dest.IntCols.Length == 1);
            Assert.IsTrue(dest.StringCols.Length == 1);
            Assert.AreEqual(dest.IntCols[0], 1);
            Assert.AreEqual(new ColumnType[] { ColumnType.Int, ColumnType.StringPointer }, dest.ColumnTypesOrdered);

            dest = source.Project(new int[] { 1, 0 });

            Assert.IsTrue(dest.DoubleCols.Length == 0);
            Assert.IsTrue(dest.IntCols.Length == 1);
            Assert.IsTrue(dest.StringCols.Length == 1);
            Assert.AreEqual(dest.IntCols[0], 1);
            Assert.AreEqual(dest.StringCols[0], "mystring");
            Assert.AreEqual(new ColumnType[] { ColumnType.StringPointer, ColumnType.Int}, dest.ColumnTypesOrdered);

            dest = source.Project(new int[] { 1, 0, 2 });

            Assert.IsTrue(dest.DoubleCols.Length == 1);
            Assert.IsTrue(dest.IntCols.Length == 1);
            Assert.IsTrue(dest.StringCols.Length == 1);
            Assert.AreEqual(dest.IntCols[0], 1);
            Assert.AreEqual(new ColumnType[] { ColumnType.StringPointer, ColumnType.Int, ColumnType.Double}, dest.ColumnTypesOrdered);

            dest = source.Project(new int[] { 3, 0 });

            Assert.IsTrue(dest.DoubleCols.Length == 0);
            Assert.IsTrue(dest.IntCols.Length == 2);
            Assert.IsTrue(dest.StringCols.Length == 0);
            Assert.AreEqual(dest.IntCols[0], 2);
            Assert.AreEqual(dest.IntCols[1], 1);
            Assert.AreEqual(new ColumnType[] { ColumnType.Int, ColumnType.Int }, dest.ColumnTypesOrdered);
        }
    }
}

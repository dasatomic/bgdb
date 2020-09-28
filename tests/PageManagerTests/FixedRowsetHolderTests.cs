using NUnit.Framework;
using PageManager;
using System;
using System.Linq;
using Test.Common;

namespace PageManagerTests
{
    public class FixedRowsetHolderTests
    {
        [Test]
        public void FixedRowsetGet()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);
            rs.SetRowGeneric(3, 0, 42);
            rs.SetRowGeneric(3, 1, 17);
            rs.SetRowGeneric(3, 2, 17.3);
            rs.SetRowGeneric(3, 3, new PagePointerOffsetPair(1, 17));

            rs.SetRowGeneric(17, 0, 42);
            rs.SetRowGeneric(17, 1, 17);
            rs.SetRowGeneric(17, 2, 17.3);
            rs.SetRowGeneric(17, 3, new PagePointerOffsetPair(1, 17));

            Assert.AreEqual(42, rs.GetRowGeneric<int>(3, 0));
            Assert.AreEqual(17, rs.GetRowGeneric<int>(3, 1));
            Assert.AreEqual(17.3, rs.GetRowGeneric<double>(3, 2));
            Assert.AreEqual(new PagePointerOffsetPair(1, 17), rs.GetRowGeneric<PagePointerOffsetPair>(3, 3));

            Assert.AreEqual(42, rs.GetRowGeneric<int>(17, 0));
            Assert.AreEqual(17, rs.GetRowGeneric<int>(17, 1));
            Assert.AreEqual(17.3, rs.GetRowGeneric<double>(17, 2));
            Assert.AreEqual(new PagePointerOffsetPair(1, 17), rs.GetRowGeneric<PagePointerOffsetPair>(17, 3));
        }

        [Test]
        public void FixedRowGet()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);
            rs.SetRowGeneric(3, 0, 42);
            rs.SetRowGeneric(3, 1, 17);
            rs.SetRowGeneric(3, 2, 17.3);
            rs.SetRowGeneric(3, 3, new PagePointerOffsetPair(1, 17));

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rs.GetRow(3, ref rh);

            Assert.AreEqual(42, rh.GetField<int>(0));
            Assert.AreEqual(17, rh.GetField<int>(1));
            Assert.AreEqual(17.3, rh.GetField<double>(2));
            Assert.AreEqual(new PagePointerOffsetPair(1, 17), rh.GetField<PagePointerOffsetPair>(3));
        }

        [Test]
        public void FixedRowSet()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            rs.SetRow(0, rh);
            rs.SetRow(1, rh);

            Assert.AreEqual(1, rs.GetRowGeneric<int>(0, 0));
            Assert.AreEqual(2, rs.GetRowGeneric<int>(0, 1));
            Assert.AreEqual(3.1, rs.GetRowGeneric<double>(0, 2));
            Assert.AreEqual(new PagePointerOffsetPair(5, 5), rs.GetRowGeneric<PagePointerOffsetPair>(0, 3));
        }

        [Test]
        public void InsertRow()
        {
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            rs.InsertRow(rh);
            rs.InsertRow(rh);
            rs.InsertRow(rh);
            rs.InsertRow(rh);

            for (int i = 0; i < 4; i++)
            {
                Assert.AreEqual(1, rs.GetRowGeneric<int>(i, 0));
                Assert.AreEqual(2, rs.GetRowGeneric<int>(i, 1));
                Assert.AreEqual(3.1, rs.GetRowGeneric<double>(i, 2));
                Assert.AreEqual(new PagePointerOffsetPair(5, 5), rs.GetRowGeneric<PagePointerOffsetPair>(i, 3));
            }
        }

        [Test]
        public void DepleteStorageInsert()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            int freeSpace = rs.FreeSpaceForItems();

            for (int i = 0; i < rs.MaxRowCount(); i++)
            {
                Assert.AreNotEqual(-1, rs.InsertRow(rh));
                Assert.AreEqual(freeSpace - i - 1, rs.FreeSpaceForItems());
            }

            Assert.AreEqual(-1, rs.InsertRow(rh));
        }

        [Test]
        public void InitFromExistingMemoryChunk()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);

            RowHolderFixed rh = new RowHolderFixed(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            Assert.AreEqual(0, rs.InsertRow(rh));
            int oldFreeSpace = rs.FreeSpaceForItems();

            RowsetHolderFixed rsnew = new RowsetHolderFixed(columnTypes, mem, false);

            RowHolderFixed rhnew = new RowHolderFixed(columnTypes);
            rsnew.GetRow(0, ref rhnew);

            Assert.AreEqual(rh, rhnew);
            Assert.AreEqual(oldFreeSpace, rsnew.FreeSpaceForItems());
        }

        [Test]
        public void Iterate()
        {
            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.StringPointer) };
            RowsetHolderFixed rs = new RowsetHolderFixed(columnTypes, mem, true);

            for (int i = 0; i < rs.MaxRowCount(); i++)
            {
                RowHolderFixed rh = new RowHolderFixed(columnTypes);
                rh.SetField<int>(0, i);
                rh.SetField<int>(1, i + 1);
                rh.SetField<double>(2, 3.1);
                rh.SetField(3, new PagePointerOffsetPair(i, i));
                Assert.AreNotEqual(-1, rs.InsertRow(rh));
            }

            var iter = rs.Iterate(columnTypes).ToArray();

            for (int i = 0; i < rs.MaxRowCount(); i++)
            {
                var rh = iter[i];
                Assert.AreEqual(i, rh.GetField<int>(0));
                Assert.AreEqual(i + 1, rh.GetField<int>(1));
                Assert.AreEqual(3.1, rh.GetField<double>(2));
                Assert.AreEqual(new PagePointerOffsetPair(i, i), rh.GetField<PagePointerOffsetPair>(3));
            }
        }

        [Test]
        public void RowHolderSetString()
        {
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.String, 10), new ColumnInfo(ColumnType.String, 5), new ColumnInfo(ColumnType.Int) };
            var rhf = new RowHolderFixed(columnTypes);

            rhf.SetField(0, "TESTTEST00".ToCharArray());
            rhf.SetField(1, "TEST0".ToCharArray());
            rhf.SetField(2, 17);

            Assert.AreEqual("TESTTEST00".ToCharArray(), rhf.GetStringField(0));
            Assert.AreEqual("TEST0".ToCharArray(), rhf.GetStringField(1));
            Assert.AreEqual(17, rhf.GetField<int>(2));
        }

        [Test]
        public void RowHolderProject()
        {
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.String, 10), new ColumnInfo(ColumnType.String, 5), new ColumnInfo(ColumnType.Int) };
            var rhf = new RowHolderFixed(columnTypes);

            rhf.SetField(0, "TESTTEST00".ToCharArray());
            rhf.SetField(1, "TEST0".ToCharArray());
            rhf.SetField(2, 17);

            var rhfnew = rhf.Project(new[] { 1, 2 });

            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(0));
            Assert.AreEqual(17, rhfnew.GetField<int>(1));
        }

        [Test]
        [Repeat(100)]
        public void RandomRowHolderProject()
        {
            ColumnInfo[] cis = GenerateDataUtils.GenerateRandomColumns(10).ToArray();
            var rhf = new RowHolderFixed(cis);

            Random r = new Random();
            int[] arr = Enumerable.Range(0, 9).ToArray();
            Randomizer.Randomize(arr);

            arr = arr.Take(r.Next(1, 10)).ToArray();

            rhf.Project(arr);
        }

        [Test]
        public void ProjectRepeatedColumn()
        {
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.String, 10), new ColumnInfo(ColumnType.String, 5), new ColumnInfo(ColumnType.Int) };
            var rhf = new RowHolderFixed(columnTypes);

            rhf.SetField(0, "TESTTEST00".ToCharArray());
            rhf.SetField(1, "TEST0".ToCharArray());
            rhf.SetField(2, 17);

            var rhfnew = rhf.Project(new[] { 1, 1, 2, 2 });

            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(0));
            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(1));
            Assert.AreEqual(17, rhfnew.GetField<int>(2));
            Assert.AreEqual(17, rhfnew.GetField<int>(3));
        }
    }
}

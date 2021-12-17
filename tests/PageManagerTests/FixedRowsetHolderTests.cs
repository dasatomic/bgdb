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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);
            rs.SetRowGeneric(3, 0, 42);
            rs.SetRowGeneric(3, 1, 17);
            rs.SetRowGeneric(3, 2, 17.3);
            rs.SetRowGeneric(3, 3, new PagePointerOffsetPair(1, 17));

            RowHolder rh = new RowHolder(columnTypes);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);

            RowHolder rh = new RowHolder(columnTypes);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);

            RowHolder rh = new RowHolder(columnTypes);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);

            RowHolder rh = new RowHolder(columnTypes);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);

            RowHolder rh = new RowHolder(columnTypes);
            rh.SetField<int>(0, 1);
            rh.SetField<int>(1, 2);
            rh.SetField<double>(2, 3.1);
            rh.SetField(3, new PagePointerOffsetPair(5, 5));

            Assert.AreEqual(0, rs.InsertRow(rh));
            int oldFreeSpace = rs.FreeSpaceForItems();

            RowsetHolder rsnew = new RowsetHolder(columnTypes, mem, false);

            RowHolder rhnew = new RowHolder(columnTypes);
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
            RowsetHolder rs = new RowsetHolder(columnTypes, mem, true);

            for (int i = 0; i < rs.MaxRowCount(); i++)
            {
                RowHolder rh = new RowHolder(columnTypes);
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
            var rhf = new RowHolder(columnTypes);

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
            var rhf = new RowHolder(columnTypes);

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
            var rhf = new RowHolder(cis);

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
            var rhf = new RowHolder(columnTypes);

            rhf.SetField(0, "TESTTEST00".ToCharArray());
            rhf.SetField(1, "TEST0".ToCharArray());
            rhf.SetField(2, 17);

            var rhfnew = rhf.Project(new[] { 1, 1, 2, 2 });

            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(0));
            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(1));
            Assert.AreEqual(17, rhfnew.GetField<int>(2));
            Assert.AreEqual(17, rhfnew.GetField<int>(3));
        }

        [Test]
        public void ProjectAndExtend()
        {
            var columnTypes = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.String, 10), new ColumnInfo(ColumnType.String, 5), new ColumnInfo(ColumnType.Int) };
            var rhf = new RowHolder(columnTypes);

            rhf.SetField(0, "TESTTEST00".ToCharArray());
            rhf.SetField(1, "TEST0".ToCharArray());
            rhf.SetField(2, 17);

            var rhfnew = rhf.ProjectAndExtend(new ProjectExtendInfo(
                new[] { ProjectExtendInfo.MappingType.Projection, ProjectExtendInfo.MappingType.Extension, ProjectExtendInfo.MappingType.Extension, ProjectExtendInfo.MappingType.Projection },
                new[] { 2, 1 },
                new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 10) }));

            rhfnew.SetField<int>(1, 42);
            rhfnew.SetField(2, "NEWTEST".ToCharArray());

            Assert.AreEqual(17, rhfnew.GetField<int>(0));
            Assert.AreEqual(42, rhfnew.GetField<int>(1));
            Assert.AreEqual("NEWTEST".ToCharArray(), rhfnew.GetStringField(2));
            Assert.AreEqual("TEST0".ToCharArray(), rhfnew.GetStringField(3));
        }

        [Test]
        public void Merge()
        {

            var columnTypes1 = new ColumnInfo[] { 
                new ColumnInfo(ColumnType.String, 10), new ColumnInfo(ColumnType.String, 5), new ColumnInfo(ColumnType.Int) };
            var rhf1 = new RowHolder(columnTypes1);

            rhf1.SetField(0, "TESTTEST00".ToCharArray());
            rhf1.SetField(1, "TEST0".ToCharArray());
            rhf1.SetField(2, 17);

            var columnTypes2 = new ColumnInfo[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 5) };
            var rhf2 = new RowHolder(columnTypes2);

            rhf2.SetField(0, 22);
            rhf2.SetField(1, "TESTN".ToCharArray());

            var rhfMerged = rhf1.Merge(rhf2);
            Assert.AreEqual("TESTTEST00".ToCharArray(), rhfMerged.GetStringField(0));
            Assert.AreEqual("TEST0".ToCharArray(), rhfMerged.GetStringField(1));
            Assert.AreEqual(17, rhfMerged.GetField<int>(2));
            Assert.AreEqual(22, rhfMerged.GetField<int>(3));
            Assert.AreEqual("TESTN".ToCharArray(), rhfMerged.GetStringField(4));
        }

        [Test]
        public void InsertOrderedSingleElem()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new System.Memory<byte>(new byte[4096]);

            RowsetHolder rs = new RowsetHolder(schema, mem, true);
            var rhf = new RowHolder(schema);

            rhf.SetField(0, 1);
            rs.InsertRowOrdered(rhf, schema, 0);

            Assert.AreEqual(1, rs.GetRowGeneric<int>(0, 1));
        }

        [Test]
        public void InsertOrderedMultipleSeq()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            for (int i = 0; i < 100; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i);
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(100, rs.GetRowCount());

            int expected = 0;
            foreach (RowHolder rh in rs.Iterate(schema))
            {
                Assert.AreEqual(expected++, rh.GetField<int>(0));
            }
        }

        [Test]
        public void InsertOrderedMultipleSeqReverse()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            for (int i = 99; i >= 0; i--)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i);
                int pos = rs.InsertRowOrdered(rhf, schema, 0);

                // It should always end up on the beginning.
                Assert.AreEqual(0, pos);
            }

            Assert.AreEqual(100, rs.GetRowCount());

            int expected = 0;
            foreach (RowHolder rh in rs.Iterate(schema))
            {
                Assert.AreEqual(expected++, rh.GetField<int>(0));
            }
        }

        [Test]
        public void InsertOrderedMultipleSeqReverseMultiColumn()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Double),
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            for (int i = 99; i >= 0; i--)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i);
                rhf.SetField(1, i * 1.1);
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(100, rs.GetRowCount());

            int expected = 0;
            foreach (RowHolder rh in rs.Iterate(schema))
            {
                Assert.AreEqual(expected, rh.GetField<int>(0));
                Assert.AreEqual(expected * 1.1, rh.GetField<double>(1));
                expected++;
            }
        }

        [Test]
        [Repeat(10)]
        public void InsertOrderedRandom()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            int maxRowCount = rs.MaxRowCount();
            int[] elemsToInsert = new int[maxRowCount];

            Random r = new Random();
            for (int i = 0; i < maxRowCount; i++)
            {
                elemsToInsert[i] = r.Next();
            }

            int[] sortedArray = elemsToInsert.OrderBy(x => x).ToArray();

            for (int i = 0; i < maxRowCount; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, elemsToInsert[i]);
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(maxRowCount, rs.GetRowCount());

            int pos = 0;
            foreach (RowHolder rh in rs.Iterate(schema))
            {
                Assert.AreEqual(sortedArray[pos], rh.GetField<int>(0));
                pos++;
            }
        }

        [Test]
        [Repeat(10)]
        public void InsertOrderedRandomMultipleColumns()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Double),
                new ColumnInfo(ColumnType.String, 10),
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            int maxRowCount = rs.MaxRowCount();
            int[] elemsToInsert = new int[maxRowCount];

            Random r = new Random();
            for (int i = 0; i < maxRowCount; i++)
            {
                elemsToInsert[i] = r.Next();
            }

            int[] sortedArray = elemsToInsert.OrderBy(x => x).ToArray();

            for (int i = 0; i < maxRowCount; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, elemsToInsert[i]);
                rhf.SetField(1, elemsToInsert[i] * 1.1);
                rhf.SetField(2, elemsToInsert[i].ToString().ToCharArray());
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(maxRowCount, rs.GetRowCount());

            int pos = 0;
            foreach (RowHolder rh in rs.Iterate(schema))
            {
                Assert.AreEqual(sortedArray[pos], rh.GetField<int>(0));
                Assert.AreEqual(sortedArray[pos] * 1.1, rh.GetField<double>(1));
                Assert.AreEqual(sortedArray[pos].ToString(), new string(rh.GetStringField(2)));
                pos++;
            }
        }

        [Test]
        public void PageSplit()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            int maxRowCount = rs.MaxRowCount();
            if (maxRowCount % 2 != 1)
            {
                maxRowCount--;
            }

            for (int i = 0; i < maxRowCount; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i);
                int pos = rs.InsertRowOrdered(rhf, schema, 0);
            }

            Memory<byte> newPageContent = new Memory<byte>(new byte[4096]);
            var rhSplit = new RowHolder(schema);
            rs.SplitPage(newPageContent, ref rhSplit, maxRowCount / 2);
            RowsetHolder rowsetSplit = new RowsetHolder(schema, newPageContent, false);

            Assert.AreEqual(maxRowCount / 2, rs.GetRowCount());
            Assert.AreEqual(maxRowCount / 2, rowsetSplit.GetRowCount());
            // Split value.
            Assert.AreEqual(maxRowCount / 2, rhSplit.GetField<int>(0));

            RowHolder[] firstPageContent = rs.Iterate(schema).ToArray();
            RowHolder[] secondPageContent = rowsetSplit.Iterate(schema).ToArray();

            for (int i = 0; i < maxRowCount / 2; i++)
            {
                Assert.AreEqual(i, firstPageContent[i].GetField<int>(0));
                Assert.AreEqual(maxRowCount / 2 + i + 1, secondPageContent[i].GetField<int>(0));
            }
        }

        [Test]
        public void BinarySearchFindOrBiggestSmallerFindElem()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            for (int i = 0; i < 100; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i);
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(100, rs.GetRowCount());

            for (int i = 0; i < 100; i++)
            {
                int pos = rs.BinarySearchFindOrBiggestSmaller<int>(i, 0, 0, rs.GetRowCount() - 1);
                Assert.AreEqual(i, pos);
            }
        }

        [Test]
        public void BinarySearchFindOrBiggestSmallerNoElem()
        {
            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Memory<byte> mem = new Memory<byte>(new byte[4096]);
            RowsetHolder rs = new RowsetHolder(schema, mem, true);

            for (int i = 0; i < 100; i++)
            {
                var rhf = new RowHolder(schema);

                rhf.SetField(0, i * 2);
                rs.InsertRowOrdered(rhf, schema, 0);
            }

            Assert.AreEqual(100, rs.GetRowCount());

            for (int i =  -2; i < 204; i++)
            {
                int expected;
                if (i < 0)
                {
                    expected = -1;
                }
                else if (i % 2 == 0 && i <= 198)
                {
                    expected = i;
                }
                else if (i % 2 == 1 && i < 200)
                {
                    expected = i - 1;
                }
                else
                {
                    expected = 198;
                }

                int pos = rs.BinarySearchFindOrBiggestSmaller<int>(i, 0, 0, rs.GetRowCount() - 1);
                if (expected == -1)
                {
                    Assert.IsTrue(pos == -1);
                }
                else
                {
                    Assert.AreEqual(expected, rs.GetRowGeneric<int>(pos, 0));
                }
            }
        }
    }
}

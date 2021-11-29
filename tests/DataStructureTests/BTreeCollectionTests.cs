using DataStructures;
using Moq;
using NUnit.Framework;
using PageManager;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Test.Common;
using DataStructures.Exceptions;

namespace DataStructureTests
{
    public class BTreeCollectionTests
    {
        private Mock<IAllocateMixedPage> pageManagerMock;
        private IAllocateMixedPage mixedPageAlloc;
        List<MixedPage> pagePointersOrdered = new List<MixedPage>();

        [SetUp]
        public void Setup()
        {
            this.pagePointersOrdered.Clear();
            this.pageManagerMock = new Mock<IAllocateMixedPage>();
            this.mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            this.pageManagerMock.Setup(pm =>
                pm.AllocateMixedPage(
                    It.IsAny<ColumnInfo[]>(),
                    It.IsAny<ulong>(),
                    It.IsAny<ulong>(),
                    It.IsAny<ITransaction>()))
                .Returns(async (ColumnInfo[] ci, ulong prev, ulong next, ITransaction tran) =>
                {
                    var page = await this.mixedPageAlloc.AllocateMixedPage(ci, prev, next, tran);
                    this.pagePointersOrdered.Add(page);
                    return page;
                });

            this.pageManagerMock.Setup(pm =>
                pm.GetMixedPage(It.IsAny<ulong>(), It.IsAny<ITransaction>(), It.IsAny<ColumnInfo[]>()))
                .Returns(async (ulong pageId, ITransaction tran, ColumnInfo[] columnInfo) =>
                {
                    return await this.mixedPageAlloc.GetMixedPage(pageId, tran, columnInfo);
                });
        }

        [Test]
        public async Task InsertSingleElem()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            var row = new RowHolder(schema);

            row.SetField(0, 42);
            await collection.Add(row, tran);

            // Single insert should result in only 1 page.
            Assert.AreEqual(1, this.pagePointersOrdered.Count);

            MixedPage root = this.pagePointersOrdered[0];

            RowHolder[] rhs = root.Fetch(tran).ToArray();
            int data = rhs[0].GetField<int>(0);
            Assert.AreEqual(42, data);

            ulong pointer = rhs[0].GetField<ulong>(1);
            // Not child elements at this moment.
            Assert.AreEqual(PageManagerConstants.NullPageId, pointer);
            Assert.AreEqual(PageManagerConstants.NullPageId, root.PrevPageId());

            bool isLeaf = (root.NextPageId() & 1UL) == 1;
            Assert.AreEqual(true, isLeaf);
        }

        [Test]
        public async Task InsertTillSplit()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            var rootPage = this.pagePointersOrdered[0];

            uint maxRowCount = rootPage.MaxRowCount();

            if (maxRowCount % 2 == 0)
            {
                maxRowCount--;
            }

            for (int i = 0; i < maxRowCount; i++)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            Assert.AreEqual(1, this.pagePointersOrdered.Count);

            MixedPage root = this.pagePointersOrdered[0];

            RowHolder[] rhs = root.Fetch(tran).ToArray();
            for (int i = 0; i < maxRowCount; i++)
            {
                int data = rhs[i].GetField<int>(0);
                Assert.AreEqual(i, data);

                ulong pointer = rhs[0].GetField<ulong>(1);
                // Not child elements at this moment.
                Assert.AreEqual(PageManagerConstants.NullPageId, pointer);
            }

            Assert.AreEqual(PageManagerConstants.NullPageId, root.PrevPageId());
            bool isLeaf = (root.NextPageId() & 1UL) == 1;
            Assert.AreEqual(true, isLeaf);
        }

        [Test]
        public async Task PageSplit()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            var rootPage = this.pagePointersOrdered[0];

            uint maxRowCountPerPage = rootPage.MaxRowCount();

            if (maxRowCountPerPage % 2 == 0)
            {
                maxRowCountPerPage--;
            }

            for (int i = 0; i < maxRowCountPerPage + 1; i++)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            // There should be three pages.
            Assert.AreEqual(3, this.pagePointersOrdered.Count);

            MixedPage page0 = this.pagePointersOrdered[0];
            MixedPage page1 = this.pagePointersOrdered[1];
            MixedPage page2 = this.pagePointersOrdered[2];

            // Initial page will host first half of the rows.
            RowHolder[] rhs = page0.Fetch(tran).ToArray();
            Assert.AreEqual(maxRowCountPerPage / 2, rhs.Length);
            for (int i = 0; i < maxRowCountPerPage / 2; i++)
            {
                int data = rhs[i].GetField<int>(0);
                Assert.AreEqual(i, data);

                ulong pointer = rhs[0].GetField<ulong>(1);
                // No child elements at this moment.
                Assert.AreEqual(PageManagerConstants.NullPageId, pointer);
            }

            Assert.AreEqual(PageManagerConstants.NullPageId, page0.PrevPageId());
            bool isLeaf = (page0.NextPageId() & 1UL) == 1;
            Assert.AreEqual(true, isLeaf);

            // second page will host second half of the rows.
            rhs = page1.Fetch(tran).ToArray();
            Assert.AreEqual(maxRowCountPerPage / 2 + 1, rhs.Length);
            int rhsPos = 0;
            for (int i = (int)maxRowCountPerPage / 2 + 1; i < maxRowCountPerPage + 1; i++)
            {
                int data = rhs[rhsPos].GetField<int>(0);
                Assert.AreEqual(i, data);

                ulong pointer = rhs[rhsPos].GetField<ulong>(1);
                // No child elements at this moment.
                Assert.AreEqual(PageManagerConstants.NullPageId, pointer);
                rhsPos++;
            }

            Assert.AreEqual(PageManagerConstants.NullPageId, page1.PrevPageId());
            isLeaf = (page1.NextPageId() & 1UL) == 1;
            Assert.AreEqual(true, isLeaf);

            // third page is new root.
            // second page will host second half of the rows.
            rhs = page2.Fetch(tran).ToArray();
            Assert.AreEqual(1, rhs.Length);

            int rootData = rhs[0].GetField<int>(0);
            Assert.AreEqual(maxRowCountPerPage / 2, rootData);

            ulong rootRightPointer = rhs[0].GetField<ulong>(1);
            Assert.AreEqual(page1.PageId(), rootRightPointer);
            ulong rootLeftPointer = page2.PrevPageId();
            Assert.AreEqual(page0.PageId(), rootLeftPointer);

            isLeaf = (page2.NextPageId() & 1UL) == 1;
            Assert.AreEqual(false, isLeaf);
        }

        [Test]
        public async Task IterateInsertSequential()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            Func<MixedPage, string> debugPrint = (page) =>
            {
                string info = $"pageId {page.PageId()}, prev: {page.PrevPageId()}\n";
                RowHolder[] rhs = page.Fetch(tran).ToArray();
                info += $"total elem count: {rhs.Length}\n";

                foreach (RowHolder rh in rhs)
                {
                    info += $"item: {rh.GetField<int>(0)}, pointer: {rh.GetField<ulong>(1)}\n";
                }

                return info;
            };

            Func<RowHolder, string> debugRowPrint = (rh) =>
            {
                string info = $"data = {rh.GetField<int>(0)}, key = {rh.GetField<ulong>(1)}";
                return info;
            };

            const int rowCount = 10000;

            for (int i = 0; i < rowCount; i++)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            int pos = 0;
            await foreach (RowHolder rh in collection.Iterate(tran))
            {
                int item = rh.GetField<int>(0);
                Assert.AreEqual(pos, item);
                pos++;
            }
        }

        [Test]
        public async Task IterateInsertReverse()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            const int rowCount = 10000;

            for (int i = rowCount - 1; i >= 0; i--)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            int pos = 0;
            await foreach (RowHolder rh in collection.Iterate(tran))
            {
                int item = rh.GetField<int>(0);
                Assert.AreEqual(pos, item);
                pos++;
            }
        }

        [Test]
        public async Task IterateInsertRandom()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            const int rowCount = 10000;
            int[] rowsToInsert = Enumerable.Range(0, rowCount).ToArray();

            Random rnd = new Random();
            int[] randomPermutation = rowsToInsert.OrderBy(x => rnd.Next()).ToArray();

            for (int i = 0; i < rowCount; i++)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            int pos = 0;
            await foreach (RowHolder rh in collection.Iterate(tran))
            {
                int item = rh.GetField<int>(0);
                Assert.AreEqual(pos, item);
                pos++;
            }
        }

        [Test]
        public async Task SeekValueExists()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            const int rowCount = 10000;
            for (int i = 0; i < rowCount; i++)
            {
                var row = new RowHolder(schema);

                row.SetField(0, i);
                await collection.Add(row, tran);
            }

            Random rnd = new Random();
            for (int i = 0; i < 10; i++)
            {
                int valToSeek = rnd.Next(0, rowCount - 1);
                await foreach (RowHolder rh in collection.Seek<int>(valToSeek, tran))
                {
                    int item = rh.GetField<int>(0);
                    Assert.AreEqual(valToSeek, item);
                }
            }
        }

        [Test]
        public async Task SeekValueRandom()
        {
            using ITransaction tran = new DummyTran();

            var schema = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int)
            };

            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0));

            BTreeCollection collection =
                new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

            Random rnd = new Random();
            const int rowCount = 10000;
            List<int> insertedValues = new List<int>();

            // exclude 10 ids.
            List<int> excludedValues = new List<int>();
            for (int i = 0; i < 10; i++)
            {
                excludedValues.Add(rnd.Next(0, rowCount));
            }


            for (int i = 0; i < rowCount; i++)
            {
                var row = new RowHolder(schema);
                int valToInsert = rnd.Next(0, rowCount * 2);
                if (i < 10)
                {
                    insertedValues.Add(valToInsert);
                }

                row.SetField(0, valToInsert);
                try
                {
                    if (!excludedValues.Contains(valToInsert))
                    {
                        await collection.Add(row, tran);
                    }
                }
                catch (KeyAlreadyExists)
                {
                    // Just ignore
                }
            }

            for (int i = 0; i < 10; i++)
            {
                int valToSeek = insertedValues.ElementAt(i);
                await foreach (RowHolder rh in collection.Seek<int>(valToSeek, tran))
                {
                    int item = rh.GetField<int>(0);
                    Assert.AreEqual(valToSeek, item);
                }
            }

            foreach (int val in excludedValues)
            {
                Assert.ThrowsAsync<KeyNotFound>(async () =>
                {
                    await foreach (RowHolder rh in collection.Seek<int>(val, tran))
                    {
                    }
                });
            }
        }
    }
}

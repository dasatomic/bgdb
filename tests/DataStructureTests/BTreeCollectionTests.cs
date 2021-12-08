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
        public async Task InsertDuplicate()
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
            Assert.ThrowsAsync<KeyAlreadyExists>(async () => await collection.Add(row, tran));
        }

        public enum GenerationStrategy
        {
            Seq,
            Rev,
            Rand,
            FromFile,
        }

        private List<int> GenerateItems(GenerationStrategy strat, int itemNum)
        {
            switch (strat)
            {
                case GenerationStrategy.Seq:
                    return Enumerable.Range(0, itemNum).ToList();
                case GenerationStrategy.Rev:
                    return Enumerable.Range(0, itemNum).Reverse().ToList();
                case GenerationStrategy.Rand:
                    Random rnd = new Random();
                    return Enumerable.Range(0, itemNum).OrderBy(x => rnd.Next()).Distinct().ToList();
                case GenerationStrategy.FromFile:
                    // used to repo issues.
                    // return File.ReadAllLines("D:\\temp.txt").Select(ln => Int32.Parse(ln)).ToList();
                default:
                    throw new ArgumentException();
            }
        }

        [Test, Pairwise]
        public async Task IterateInsert(
            [Values(GenerationStrategy.Seq, GenerationStrategy.Rev, GenerationStrategy.Rand)] GenerationStrategy strat,
            [Values(ColumnType.Int, ColumnType.Double)] ColumnType columnType)
        {
            using ITransaction tran = new DummyTran();


            Func<RowHolder, RowHolder, int> comp = (rh1, rh2) =>
                columnType == ColumnType.Int ?
                rh1.GetField<int>(0).CompareTo(rh2.GetField<int>(0)) :
                rh1.GetField<double>(0).CompareTo(rh2.GetField<double>(0));

            const int rowCount = 10000;

            // used for debugging.
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

            if (columnType == ColumnType.Int)
            {
                var schema = new ColumnInfo[]
                {
                    new ColumnInfo(ColumnType.Int)
                };

                int[] generatedItems = GenerateItems(strat, rowCount).ToArray();
                BTreeCollection collection =
                    new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);
                foreach (int item in generatedItems)
                {
                    var row = new RowHolder(schema);

                    row.SetField(0, item);
                    await collection.Add(row, tran);
                }

                // sort for verification.
                Array.Sort(generatedItems);

                int pos = 0;
                await foreach (RowHolder rh in collection.Iterate(tran))
                {
                    int item = rh.GetField<int>(0);
                    Assert.AreEqual(generatedItems[pos], item);
                    pos++;
                }
            }
            else
            {
                var schema = new ColumnInfo[]
                {
                    new ColumnInfo(ColumnType.Double)
                };

                BTreeCollection collection =
                    new BTreeCollection(pageManagerMock.Object, schema, new DummyTran(), comp, 0);

                double[] generatedItems = GenerateItems(strat, rowCount).Select(x => x * 1.1).ToArray();
                foreach (double item in generatedItems)
                {
                    var row = new RowHolder(schema);

                    row.SetField(0, item);
                    await collection.Add(row, tran);
                }

                // sort for verification.
                Array.Sort(generatedItems);

                int pos = 0;
                await foreach (RowHolder rh in collection.Iterate(tran))
                {
                    double item = rh.GetField<double>(0);
                    Assert.AreEqual(generatedItems[pos], item);
                    pos++;
                }
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

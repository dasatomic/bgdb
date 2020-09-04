using DataStructures;
using LogManager;
using NUnit.Framework;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace DataStructureTests
{
    public class PageListCollectionTests
    {
        [Test]
        public async Task InitPageList()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            using ITransaction tran = new DummyTran();
            ColumnType[] types = new[] { ColumnType.Int, ColumnType.Int };

            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, new DummyTran());

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt++;
            }

            Assert.AreEqual(0, cnt);
        }

        [Test]
        public async Task PageListInsert()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            foreach (var row in rows)
            {
                await collection.Add(row, tran);
            }

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt++;
            }

            Assert.AreEqual(rows.Count, cnt);
        }

        [Test]
        public async Task PageMultiInsert()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            for (int i = 0; i < 100; i++)
            {
                foreach (var row in rows)
                {
                    await collection.Add(row, tran);
                }
            }

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt++;
            }

            Assert.AreEqual(rows.Count * 100, cnt);
        }

        [Test]
        public async Task FilterTestNotFound()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            for (int i = 0; i < 100; i++)
            {
                foreach (var row in rows)
                {
                    await collection.Add(row, tran);
                }
            }

            await foreach (var val in collection.Where(holder => holder.GetField<int>(0) == 42, tran))
            {
                Assert.Fail("There shouldn't be any rows.");
            }
        }

        [Test]
        public async Task FilterTestFound()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            for (int i = 0; i < 100; i++)
            {
                foreach (var row in rows)
                {
                    await collection.Add(row, tran);
                }
            }

            bool found = false;
            await foreach (var val  in collection.Where((searcher) => searcher.GetField<int>(0) == rows[0].GetField<int>(0), tran))
            {
                found = true;
            }

            Assert.IsTrue(found);
        }

        [Test]
        public async Task IterationTests()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            List<int> column0Insert = new List<int>();
            for (int i = 0; i < 100; i++)
            {
                foreach (var row in rows)
                {
                    column0Insert.Add(row.GetField<int>(0));
                    await collection.Add(row, tran);
                }
            }

            List<int> column0 = new List<int>();
            await foreach (var i in collection.Iterate(tran))
            {
                column0.Add(i.GetField<int>(0));
            }

            Assert.AreEqual(column0Insert, column0);
        }
    }
}
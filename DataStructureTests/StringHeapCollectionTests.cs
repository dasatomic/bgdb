using DataStructures;
using NUnit.Framework;
using PageManager;
using System.Linq;
using Test.Common;

namespace DataStructureTests
{
    public class StringHeapCollectionTests
    {
        [Test]
        public void InitStringHeap()
        {
            var strPageAlloc =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);
        }

        [Test]
        public void ReadFromStringHeap()
        {
            var strPageAlloc =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);
            string itemToInsert = "one";
            var offset = collection.Add(itemToInsert.ToCharArray(), tran);
            StringOnlyPage stringOnlyPage = strPageAlloc.GetPageStr((uint)offset.PageId, tran);
            Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage));
        }

        [Test]
        public void ReadFromStringHeapMultiPage()
        {
            var strPageAlloc =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);

            for (int i = 0; i < 1000; i++)
            {
                string itemToInsert = i.ToString();
                var offset = collection.Add(itemToInsert.ToCharArray(), tran);
                StringOnlyPage stringOnlyPage = strPageAlloc.GetPageStr((uint)offset.PageId, tran);
                Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage));
            }
        }
    }
}

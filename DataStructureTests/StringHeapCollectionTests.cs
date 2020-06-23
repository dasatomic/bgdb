using DataStructures;
using NUnit.Framework;
using PageManager;
using System.Linq;
using System.Threading.Tasks;
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
        public async Task ReadFromStringHeap()
        {
            var strPageAlloc =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);
            string itemToInsert = "one";
            var offset = await collection.Add(itemToInsert.ToCharArray(), tran);
            StringOnlyPage stringOnlyPage = await strPageAlloc.GetPageStr((uint)offset.PageId, tran);
            Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage, tran));
        }

        [Test]
        public async Task ReadFromStringHeapMultiPage()
        {
            var strPageAlloc =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);

            for (int i = 0; i < 1000; i++)
            {
                string itemToInsert = i.ToString();
                var offset = await collection.Add(itemToInsert.ToCharArray(), tran);
                StringOnlyPage stringOnlyPage = await strPageAlloc.GetPageStr((uint)offset.PageId, tran);
                Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage, tran));
            }
        }
    }
}

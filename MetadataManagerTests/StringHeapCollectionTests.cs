using MetadataManager;
using NUnit.Framework;
using PageManager;
using System.Linq;
using Test.Common;

namespace MetadataManagerTests
{
    public class StringHeapCollectionTests
    {
        [Test]
        public void InitStringHeap()
        {
            IAllocateStringPage strPageAlloc= new InMemoryPageManager(4096);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);
        }

        [Test]
        public void ReadFromStringHeap()
        {
            IAllocateStringPage strPageAlloc= new InMemoryPageManager(4096);
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
            IAllocateStringPage strPageAlloc= new InMemoryPageManager(4096);
            DummyTran tran = new DummyTran();

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc, tran);

            for (int i = 0; i < 10000; i++)
            {
                string itemToInsert = i.ToString();
                var offset = collection.Add(itemToInsert.ToCharArray(), tran);
                StringOnlyPage stringOnlyPage = strPageAlloc.GetPageStr((uint)offset.PageId, tran);
                Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage));
            }
        }
    }
}

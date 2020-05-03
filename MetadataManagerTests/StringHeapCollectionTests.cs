using MetadataManager;
using NUnit.Framework;
using PageManager;
using System.Linq;

namespace MetadataManagerTests
{
    public class StringHeapCollectionTests
    {
        [Test]
        public void InitStringHeap()
        {
            IAllocateStringPage strPageAlloc=
                new InMemoryPageManager(4096);

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc);
        }

        [Test]
        public void ReadFromStringHeap()
        {
            IAllocateStringPage strPageAlloc=
                new InMemoryPageManager(4096);

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc);
            string itemToInsert = "one";
            var offset = collection.Add(itemToInsert.ToCharArray());
            StringOnlyPage stringOnlyPage = strPageAlloc.GetPageStr((uint)offset.PageId);
            Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage));
        }

        [Test]
        public void ReadFromStringHeapMultiPage()
        {
            IAllocateStringPage strPageAlloc=
                new InMemoryPageManager(4096);

            StringHeapCollection collection = new StringHeapCollection(strPageAlloc);

            for (int i = 0; i < 10000; i++)
            {
                string itemToInsert = i.ToString();
                var offset = collection.Add(itemToInsert.ToCharArray());
                StringOnlyPage stringOnlyPage = strPageAlloc.GetPageStr((uint)offset.PageId);
                Assert.AreEqual(itemToInsert.ToArray(), stringOnlyPage.FetchWithOffset((uint)offset.OffsetInPage));
            }
        }
    }
}

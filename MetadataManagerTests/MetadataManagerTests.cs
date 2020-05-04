using NUnit.Framework;
using MetadataManager;
using PageManager;

namespace MetadataManagerTests
{
    public class MetadataManagerTests
    {
        [Test]
        public void MetadataManagerInit()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
        }

        [Test]
        public void MetadataManagerInitFromExisting()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager.MetadataManager mm1 = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);

            MetadataManager.MetadataManager mm2 = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
        }
    }
}

using NUnit.Framework;
using MetadataManager;
using PageManager;
using LogManager;
using System.IO;
using Test.Common;

namespace MetadataManagerTests
{
    public class MetadataManagerTests
    {
        [Test]
        public void MetadataManagerInit()
        {
            var allocator =  new MemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            DummyTran tran = new DummyTran();

            StringHeapCollection stringHeap = new StringHeapCollection(allocator, tran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
        }

        [Test]
        public void MetadataManagerInitFromExisting()
        {
            var allocator =  new MemoryPageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            DummyTran tran = new DummyTran();

            StringHeapCollection stringHeap = new StringHeapCollection(allocator, tran);
            MetadataManager.MetadataManager mm1 = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            MetadataManager.MetadataManager mm2 = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
        }
    }
}

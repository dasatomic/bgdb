using PageManager;
using System;

namespace Test.Common
{
    public static class TestGlobals
    {
        public const int DefaultPageSize = 4096;

        public const int DefaultBufferPoolSizeMb = 16;

        public static IPageEvictionPolicy DefaultEviction
        {
            get
            {
                return new FifoEvictionPolicy(10, 5);
            }
        }

        public static IPageEvictionPolicy RestrictiveEviction = new FifoEvictionPolicy(1, 1);

        public static IPersistedStream DefaultPersistedStream
        {
            get
            {
                string fileName = string.Format("{0}.data", Guid.NewGuid());
                return new PersistedStream(1024 * 1024, fileName, createNew: true);
            }
        }

        public static ITransaction DummyTran = new DummyTran();

        public static Instrumentation.Logger TestFileLogger = new Instrumentation.Logger("SharedTestFile", "mainrep", Instrumentation.LogLevel.Debug);
    }
}

using NUnit.Framework;
using PageManager;
using System.Linq;
using System.Runtime.Serialization;
using Test.Common;

namespace PageManagerTests
{
    public class LongPageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void VerifyPageId()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(42, page.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(PageType.LongPage, page.PageType());
        }

        [Test]
        public void VerifyRowCount()
        {
            long[] startArray = new long[] { 1, 2, 3, 4 };
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(0, page.RowCount());
            page.Merge(startArray, new DummyTran());
            Assert.AreEqual(startArray.Length, page.RowCount());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(4096, page.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            long[] content = page.Fetch(TestGlobals.DummyTran);
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            long[] startArray = new long[] { 1, 2 };

            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            page.Merge(startArray, new DummyTran());
            long[] content = page.Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            long[] startArray = new long[] { 1, 2 };
            long[] secondArray = new long[] { 3, 4 };

            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            page.Merge(startArray, new DummyTran());
            long[] content = page.Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(startArray, content);

            page.Merge(secondArray, new DummyTran());
            content = page.Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(startArray.Concat(secondArray), content);
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

                long[] array = new long[DefaultSize / sizeof(long)];
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = i;
                }

                page.Merge(array, new DummyTran());
            });
        }
    }
}

using NUnit.Framework;
using PageManager;
using System.Runtime.Serialization;

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
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(42, page.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(PageType.LongPage, page.PageType());
        }

        [Test]
        public void VerifyRowCount()
        {
            long[] startArray = new long[] { 1, 2, 3, 4 };
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(0, page.RowCount());
            page.Store(startArray);
            Assert.AreEqual(startArray.Length, page.RowCount());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(4096, page.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            long[] content = page.Fetch();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            long[] startArray = new long[] { 1, 2 };

            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            page.Store(startArray);
            long[] content = page.Fetch();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            long[] startArray = new long[] { 1, 2 };
            long[] secondArray = new long[] { 3, 4 };

            LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);

            page.Store(startArray);
            long[] content = page.Fetch();
            Assert.AreEqual(startArray, content);

            page.Store(secondArray);
            content = page.Fetch();
            Assert.AreEqual(secondArray, content);
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                LongOnlyPage page = new LongOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);

                long[] array = new long[DefaultSize / sizeof(long)];
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = i;
                }

                page.Store(array);
            });
        }
    }
}

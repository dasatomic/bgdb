using NUnit.Framework;
using PageManager;
using System;
using System.Linq;
using System.Runtime.Serialization;
using Test.Common;

namespace PageManagerTests
{
    public class DoublePageTest
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void VerifyPageId()
        {
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(42, page.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(PageType.DoublePage, page.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(4096, page.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            double[] content = page.Fetch();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            double[] startArray = new double[] { 1, 2, 3, 4 };
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); ;
            page.Merge(startArray);
            double[] content = page.Fetch();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            double[] startArray = new double[] { 1, 2, 3, 4 };
            double[] secondArray = new double[] { 5, 6 };
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            doublePage.Merge(startArray);
            double[] content = doublePage.Fetch();
            Assert.AreEqual(startArray, content);

            doublePage.Merge(secondArray);
            content = doublePage.Fetch();
            Assert.AreEqual(startArray.Concat(secondArray), content);
        }

        [Test]
        public void VerifyInvalidParams()
        {
            Assert.Throws<ArgumentException>(() => { DoubleOnlyPage doublePage = new DoubleOnlyPage(0, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); });
            Assert.Throws<ArgumentException>(() => { DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultPageId + 1, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); });
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
                doublePage.Merge(new double[doublePage.MaxRowCount() + 1]);
            });
        }

        [Test]
        public void VerifySetMax()
        {
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            double[] startArray = new double[doublePage.MaxRowCount()];
            doublePage.Merge(startArray);
            double[] content = doublePage.Fetch();
            Assert.AreEqual(startArray, content);
        }
    }
}

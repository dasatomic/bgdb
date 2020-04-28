using NUnit.Framework;
using PageManager;
using System;
using System.Runtime.Serialization;

namespace PageManagerTests
{
    public class DoublePageTest
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;

        [Test]
        public void VerifyPageId()
        {
            DoubleOnlyPage page = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(42, page .PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(4096, doublePage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            double[] content = doublePage.Deserialize();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            double[] startArray = new double[] { 1, 2, 3, 4 };
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            doublePage.Serialize(startArray);
            double[] content = doublePage.Deserialize();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            double[] startArray = new double[] { 1, 2, 3, 4 };
            double[] secondArray = new double[] { 5, 6 };
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);

            doublePage.Serialize(startArray);
            double[] content = doublePage.Deserialize();
            Assert.AreEqual(startArray, content);

            doublePage.Serialize(secondArray);
            content = doublePage.Deserialize();
            Assert.AreEqual(secondArray, content);
        }

        [Test]
        public void VerifyInvalidParams()
        {
            Assert.Throws<ArgumentException>(() => { DoubleOnlyPage doublePage = new DoubleOnlyPage(0, DefaultPageId); });
            Assert.Throws<ArgumentException>(() => { DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultPageId + 1, DefaultPageId); });
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
                doublePage.Serialize(new double[doublePage.MaxRowCount() + 1]);
            });
        }

        [Test]
        public void VerifySetMax()
        {
            DoubleOnlyPage doublePage = new DoubleOnlyPage(DefaultSize, DefaultPageId);
            double[] startArray = new double[doublePage.MaxRowCount()];
            doublePage.Serialize(startArray);
            double[] content = doublePage.Deserialize();
            Assert.AreEqual(startArray, content);
        }
    }
}

using NUnit.Framework;
using PageManager;
using System;
using System.Runtime.Serialization;

namespace PageManagerTests
{
    public class IntPageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;

        [Test]
        public void VerifyPageId()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(42, intPage.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(PageType.IntPage, intPage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            Assert.AreEqual(4096, intPage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            int[] content = intPage.Deserialize();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            intPage.Serialize(startArray);
            int[] content = intPage.Deserialize();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyInvalidParams()
        {
            Assert.Throws<ArgumentException>(() => { IntegerOnlyPage intPage = new IntegerOnlyPage(0, DefaultPageId); });
            Assert.Throws<ArgumentException>(() => { IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultPageId + 1, DefaultPageId); });
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
                intPage.Serialize(new int[intPage.MaxRowCount() + 1]);
            });
        }

        [Test]
        public void VerifySetMax()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId);
            int[] startArray = new int[intPage.MaxRowCount()];
            intPage.Serialize(startArray);
            int[] content = intPage.Deserialize();
            Assert.AreEqual(startArray, content);
        }
    }
}
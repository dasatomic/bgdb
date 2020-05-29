using NUnit.Framework;
using PageManager;
using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Test.Common;

namespace PageManagerTests
{
    public class IntPageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void VerifyPageId()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(42, intPage.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(PageType.IntPage, intPage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(4096, intPage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            int[] content = intPage.Fetch();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            intPage.Merge(startArray);
            int[] content = intPage.Fetch();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyRowCount()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(0, intPage.RowCount());
            intPage.Merge(startArray);
            Assert.AreEqual(startArray.Length, intPage.RowCount());
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            int[] secondArray = new int[] { 5, 6 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            intPage.Merge(startArray);
            int[] content = intPage.Fetch();
            Assert.AreEqual(startArray, content);

            intPage.Merge(secondArray);
            content = intPage.Fetch();
            Assert.AreEqual(startArray.Concat(secondArray), content);
        }

        [Test]
        public void VerifyInvalidParams()
        {
            Assert.Throws<ArgumentException>(() => { IntegerOnlyPage intPage = new IntegerOnlyPage(0, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); });
            Assert.Throws<ArgumentException>(() => { IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultPageId + 1, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); });
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
                intPage.Merge(new int[intPage.MaxRowCount() + 1]);
            });
        }

        [Test]
        public void VerifySetMax()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            int[] startArray = new int[intPage.MaxRowCount()];
            intPage.Merge(startArray);
            int[] content = intPage.Fetch();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void Merge()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            int[] secondArray = new int[] { 5, 6 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            intPage.Merge(startArray);
            intPage.Merge(secondArray);

            int[] content = intPage.Fetch();
            Assert.AreEqual(startArray.Concat(secondArray).ToArray(), content);
        }

        [Test]
        public void VerifyFromStream()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            intPage.Merge(startArray);

            byte[] content = new byte[DefaultSize];

            using (var stream = new MemoryStream(content))
            {
                intPage.Persist(stream);
            }

            var source = new BinaryReader(new MemoryStream(content));
            IntegerOnlyPage pageDeserialized = new IntegerOnlyPage(source);
            Assert.AreEqual(intPage.PageId(), pageDeserialized.PageId());
            Assert.AreEqual(intPage.PageType(), pageDeserialized.PageType());
            Assert.AreEqual(intPage.RowCount(), pageDeserialized.RowCount());
            Assert.AreEqual(intPage.Fetch(), pageDeserialized.Fetch());
        }
    }
}
using NUnit.Framework;
using PageManager;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Test.Common;

namespace PageManagerTests
{
    class StringPageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void VerifyPageId()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran()); ;
            Assert.AreEqual(42, strPage.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(4096, strPage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            char[][] content = strPage.Fetch(TestGlobals.DummyTran).ToArray();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            var startArray = new List<char[]> 
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            startArray.ForEach(i => strPage.Insert(i, new DummyTran()));
            char[][] content = strPage.Fetch(TestGlobals.DummyTran).ToArray();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            var startArray = new List<char[]>
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            var secondArray = new List<char[]>
            { 
                "321".ToCharArray(),
                "1234".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            startArray.ForEach(i => strPage.Insert(i, new DummyTran()));
            char[][] content = strPage.Fetch(TestGlobals.DummyTran).ToArray();
            Assert.AreEqual(startArray, content);

            secondArray.ForEach(i => strPage.Insert(i, new DummyTran()));
            content = strPage.Fetch(TestGlobals.DummyTran).ToArray();
            Assert.AreEqual(startArray.Concat(secondArray), content);
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<NotEnoughSpaceException>(() => {
                StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

                for (int i = 0; i < DefaultSize / 4; i++)
                {
                    strPage.Insert("0123".ToCharArray(), new DummyTran());
                }
            });
        }

        [Test]
        public void VerifySetMax()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            const int arrLength = 4;

            int maxCount = (int)(strPage.MaxRowCount() / (arrLength + sizeof(short)));
            for (int i = 0; i < maxCount; i++)
            {
                strPage.Insert("0123".ToCharArray(), new DummyTran());
            }

            char[][] content = strPage.Fetch(TestGlobals.DummyTran).ToArray();

            Assert.AreEqual(Enumerable.Repeat("0123", maxCount), content);
        }

        [Test]
        public void VerifyMerge()
        {
            var startArray = new List<char[]> 
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            var secondArray = new List<char[]>
            { 
                "456".ToCharArray(),
                "1234".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            startArray.ForEach(i => strPage.Insert(i, new DummyTran()));
            secondArray.ForEach(i => strPage.Insert(i, new DummyTran()));
            char[][] result = strPage.Fetch(TestGlobals.DummyTran).ToArray();

            Assert.AreEqual(startArray.Concat(secondArray), result);
        }

        [Test]
        public void MergeWithOffset()
        {
            var startArray = new List<char[]>
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(0, strPage.RowCount());
            int offsetOne = strPage.Insert(startArray[0], TestGlobals.DummyTran);
            Assert.AreEqual(0, offsetOne);
            Assert.AreEqual(startArray[0], strPage.FetchWithOffset((uint)offsetOne, TestGlobals.DummyTran));
            Assert.AreEqual(1, strPage.RowCount());
            int offsetTwo = strPage.Insert(startArray[1], TestGlobals.DummyTran);
            Assert.AreEqual(startArray[0].Length + sizeof(short), offsetTwo);
            Assert.AreEqual(startArray[1], strPage.FetchWithOffset((uint)offsetTwo, TestGlobals.DummyTran));
            Assert.AreEqual(2, strPage.RowCount());
        }

        [Test]
        public void MergeUntilAlmostFull()
        {
            var strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            uint sizeAvailable = strPage.SizeInBytes() - IPage.FirstElementPosition;

            char[] elemToInsert = "one".ToArray();
            uint maxElemCount = (uint)(sizeAvailable / (elemToInsert.Length + sizeof(short)));

            for (uint i = 0; i < maxElemCount; i++)
            {
                strPage.Insert(elemToInsert, TestGlobals.DummyTran);
                Assert.AreEqual(i + 1, strPage.RowCount());
            }
        }

        [Test]
        public void MergeUntilFull()
        {
            var strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            uint sizeAvailable = strPage.SizeInBytes() - IPage.FirstElementPosition;

            char[] elemToInsert = "one".ToArray();
            uint maxElemCount = (uint)(sizeAvailable / (elemToInsert.Length + sizeof(short)));

            for (uint i = 0; i < maxElemCount; i++)
            {
                strPage.Insert(elemToInsert, TestGlobals.DummyTran);
                Assert.AreEqual(i + 1, strPage.RowCount());
            }

            Assert.Throws<NotEnoughSpaceException>(() => strPage.Insert(elemToInsert, TestGlobals.DummyTran));
        }

        [Test]
        public void VerifyFromStream()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            strPage.Insert(startArray[0], new DummyTran());
            strPage.Insert(startArray[1], new DummyTran());

            byte[] content = new byte[DefaultSize];

            using (var stream = new MemoryStream(content))
            using (var bw = new BinaryWriter(stream))
            {
                strPage.Persist(bw);
            }

            var source = new BinaryReader(new MemoryStream(content));
            StringOnlyPage pageDeserialized = new StringOnlyPage(source);
            Assert.AreEqual(strPage.PageId(), pageDeserialized.PageId());
            Assert.AreEqual(strPage.PageType(), pageDeserialized.PageType());
            Assert.AreEqual(strPage.RowCount(), pageDeserialized.RowCount());
            Assert.AreEqual(strPage.Fetch(TestGlobals.DummyTran), pageDeserialized.Fetch(TestGlobals.DummyTran));
        }
    }
}

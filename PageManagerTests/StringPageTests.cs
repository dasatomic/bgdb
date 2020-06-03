using NUnit.Framework;
using PageManager;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
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
            char[][] content = strPage.Fetch();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            strPage.Merge(startArray, new DummyTran());
            char[][] content = strPage.Fetch();
            Assert.AreEqual(startArray, content);
        }

        [Test]
        public void VerifyDoubleSerializeDeserialize()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            char[][] secondArray = new char[][]
            { 
                "321".ToCharArray(),
                "1234".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

            strPage.Merge(startArray, new DummyTran());
            char[][] content = strPage.Fetch();
            Assert.AreEqual(startArray, content);

            strPage.Merge(secondArray, new DummyTran());
            content = strPage.Fetch();
            Assert.AreEqual(startArray.Concat(secondArray), content);
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<NotEnoughSpaceException>(() => {
                StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());

                char[][] array = new char[DefaultSize / 4][];
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = "0123".ToCharArray();
                }

                strPage.Merge(array, new DummyTran());
            });
        }

        [Test]
        public void VerifySetMax()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            const int arrLength = 4;

            char[][] array = new char[strPage.MaxRowCount() / (arrLength + sizeof(short))][];
            for (int i = 0; i < array.Length; i++)
            {
                array[i] = "0123".ToCharArray();
            }

            strPage.Merge(array, new DummyTran());
            char[][] content = strPage.Fetch();

            Assert.AreEqual(array, content);
        }

        [Test]
        public void VerifyMerge()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            char[][] secondArray = new char[][]
            { 
                "456".ToCharArray(),
                "1234".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            strPage.Merge(startArray, new DummyTran());
            strPage.Merge(secondArray, new DummyTran());
            char[][] result = strPage.Fetch();

            Assert.AreEqual(startArray.Concat(secondArray), result);
        }

        [Test]
        public void MergeWithOffset()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(0, strPage.RowCount());
            uint offsetOne = strPage.MergeWithOffsetFetch(startArray[0]);
            Assert.AreEqual(IPage.FirstElementPosition, offsetOne);
            Assert.AreEqual(startArray[0], strPage.FetchWithOffset(offsetOne));
            Assert.AreEqual(1, strPage.RowCount());
            uint offsetTwo = strPage.MergeWithOffsetFetch(startArray[1]);
            Assert.AreEqual(IPage.FirstElementPosition + startArray[0].Length + sizeof(short), offsetTwo);
            Assert.AreEqual(startArray[1], strPage.FetchWithOffset(offsetTwo));
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
                strPage.MergeWithOffsetFetch(elemToInsert);
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
                strPage.MergeWithOffsetFetch(elemToInsert);
                Assert.AreEqual(i + 1, strPage.RowCount());
            }

            Assert.Throws<NotEnoughSpaceException>(() => strPage.MergeWithOffsetFetch(elemToInsert));
        }

        [Test]
        public void PageCoruptionTest()
        {
            char[][] startArray = new char[][]
            { 
                "123".ToCharArray(),
                "4321".ToCharArray(),
            };

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage, new DummyTran());
            Assert.AreEqual(0, strPage.RowCount());
            uint offsetOne = strPage.MergeWithOffsetFetch(startArray[0]);
            uint offsetTwo = strPage.MergeWithOffsetFetch(startArray[1]);
            Assert.Throws<PageCorruptedException>(() => strPage.FetchWithOffset(offsetOne + 1));
            Assert.Throws<PageCorruptedException>(() => strPage.FetchWithOffset(offsetTwo + 1));
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
            strPage.Merge(startArray, new DummyTran());

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
            Assert.AreEqual(strPage.Fetch(), pageDeserialized.Fetch());
        }
    }
}

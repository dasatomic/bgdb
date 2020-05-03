using NUnit.Framework;
using PageManager;
using System.Linq;
using System.Runtime.Serialization;

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
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(42, strPage.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(4096, strPage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            char[][] content = strPage.Deserialize();
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

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            strPage.Serialize(startArray);
            char[][] content = strPage.Deserialize();
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

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);

            strPage.Serialize(startArray);
            char[][] content = strPage.Deserialize();
            Assert.AreEqual(startArray, content);

            strPage.Serialize(secondArray);
            content = strPage.Deserialize();
            Assert.AreEqual(secondArray, content);
        }

        [Test]
        public void VerifySetMoreThanMax()
        {
            Assert.Throws<SerializationException>(() => {
                StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);

                char[][] array = new char[DefaultSize / 4][];
                for (int i = 0; i < array.Length; i++)
                {
                    array[i] = "0123".ToCharArray();
                }

                strPage.Serialize(array);
            });
        }

        [Test]
        public void VerifySetMax()
        {
            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);

            char[][] array = new char[strPage.MaxRowCount() / 5][];
            for (int i = 0; i < array.Length; i++)
            {
                array[i] = "0123".ToCharArray();
            }

            strPage.Serialize(array);
            char[][] content = strPage.Deserialize();

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

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            strPage.Serialize(startArray);
            strPage.Merge(secondArray);
            char[][] result = strPage.Deserialize();

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

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(0, strPage.RowCount());
            uint offsetOne = strPage.MergeWithOffsetFetch(startArray[0]);
            Assert.AreEqual(IPage.FirstElementPosition, offsetOne);
            Assert.AreEqual(startArray[0], strPage.FetchWithOffset(offsetOne));
            Assert.AreEqual(1, strPage.RowCount());
            uint offsetTwo = strPage.MergeWithOffsetFetch(startArray[1]);
            Assert.AreEqual(IPage.FirstElementPosition + startArray[0].Length + 1, offsetTwo);
            Assert.AreEqual(startArray[1], strPage.FetchWithOffset(offsetTwo));
            Assert.AreEqual(2, strPage.RowCount());
        }

        [Test]
        public void MergeUntilAlmostFull()
        {
            var strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            uint sizeAvailable = strPage.SizeInBytes() - IPage.FirstElementPosition;

            char[] elemToInsert = "one".ToArray();
            uint maxElemCount = (uint)(sizeAvailable / (elemToInsert.Length + 1));

            for (uint i = 0; i < maxElemCount; i++)
            {
                strPage.MergeWithOffsetFetch(elemToInsert);
                Assert.AreEqual(i + 1, strPage.RowCount());
            }
        }

        [Test]
        public void MergeUntilFull()
        {
            var strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            uint sizeAvailable = strPage.SizeInBytes() - IPage.FirstElementPosition;

            char[] elemToInsert = "one".ToArray();
            uint maxElemCount = (uint)(sizeAvailable / (elemToInsert.Length + 1));

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

            StringOnlyPage strPage = new StringOnlyPage(DefaultSize, DefaultPageId, DefaultPrevPage, DefaultNextPage);
            Assert.AreEqual(0, strPage.RowCount());
            uint offsetOne = strPage.MergeWithOffsetFetch(startArray[0]);
            uint offsetTwo = strPage.MergeWithOffsetFetch(startArray[1]);
            Assert.Throws<PageCorruptedException>(() => strPage.FetchWithOffset(offsetOne + 1));
            Assert.Throws<PageCorruptedException>(() => strPage.FetchWithOffset(offsetTwo + 1));
        }
    }
}

using NUnit.Framework;
using PageManager;

namespace PageManagerTests
{
    public class IntPageTests
    {

        [Test]
        public void VerifyPageId()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(4096, 42);
            Assert.AreEqual(42, intPage.PageId());
        }

        [Test]
        public void VerifyPageType()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(4096, 42);
            Assert.AreEqual(PageType.IntPage, intPage.PageType());
        }

        [Test]
        public void VerifySizeInBytes()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(4096, 42);
            Assert.AreEqual(4096, intPage.SizeInBytes());
        }

        [Test]
        public void VerifyDeserializationEmpty()
        {
            IntegerOnlyPage intPage = new IntegerOnlyPage(4096, 42);
            int[] content = intPage.Deserialize();
            Assert.IsTrue(content.Length == 0);
        }

        [Test]
        public void VerifySerializeDeserialize()
        {
            int[] startArray = new int[] { 1, 2, 3, 4 };
            IntegerOnlyPage intPage = new IntegerOnlyPage(4096, 42);
            intPage.Serialize(startArray);
            int[] content = intPage.Deserialize();
            Assert.AreEqual(startArray, content);
        }
    }
}
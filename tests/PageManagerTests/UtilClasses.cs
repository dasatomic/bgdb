using NUnit.Framework;
using PageManager.UtilStructures;

namespace PageManagerTests
{
    public class UtilClasses
    {
        [Test]
        public void IntDivCeil()
        {
            Assert.AreEqual(3, IntCeil.CeilDiv(10, 4));
            Assert.AreEqual(2, IntCeil.CeilDiv(10, 5));
            Assert.AreEqual(4, IntCeil.CeilDiv(10, 3));
        }
    }
}

using NUnit.Framework;
using PageManager;
using PageManager.UtilStructures;
using System;

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

        [Test]
        public void ArrayShiftLeft()
        {
            Memory<byte> mem = new Memory<byte>(new byte[] { 1, 0, 3, 4 });
            ByteSliceOperations.ShiftSlice(mem, 2, 1, 2);

            Assert.AreEqual(mem.ToArray(), new byte[] { 1, 3, 4, 4 });
        }

        [Test]
        public void ArrayShiftOver()
        {
            Memory<byte> mem = new Memory<byte>(new byte[] { 0, 2, 3, 4 });
            ByteSliceOperations.ShiftSlice(mem, 1, 0, 3);

            Assert.AreEqual(mem.ToArray(), new byte[] { 2, 3, 4, 4 });
        }

        [Test]
        public void ArrayShiftRight()
        {
            Memory<byte> mem = new Memory<byte>(new byte[] { 1, 2, 0, 4 });
            ByteSliceOperations.ShiftSlice(mem, 0, 1, 2);

            Assert.AreEqual(mem.ToArray(), new byte[] { 1, 1, 2, 4 });
        }

        [Test]
        public void CharrArrayEq()
        {
            string abc = "abcd";
            CharrArray cr = new CharrArray(abc.ToCharArray());

            Assert.AreEqual(0, cr.CompareToString(abc));
            Assert.AreEqual(0, CharrArray.Compare(abc.ToCharArray(), abc));
        }

        [Test]
        public void CharrArrayBigger()
        {
            string abc = "abcd";
            CharrArray cr = new CharrArray(abc.ToCharArray());

            Assert.AreEqual(1, cr.CompareToString("aaaa"));
            Assert.AreEqual(1, CharrArray.Compare(abc, "aaaa"));
        }

        [Test]
        public void CharrArraySmaller()
        {
            string abc = "abcd";
            CharrArray cr = new CharrArray(abc.ToCharArray());

            Assert.AreEqual(-1, cr.CompareToString("abcf"));
            Assert.AreEqual(-1, CharrArray.Compare(abc, "abcf"));
        }
    }
}

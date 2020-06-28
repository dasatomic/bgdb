using DataStructures;
using NUnit.Framework;
using System;

namespace DataStructureTests
{
    public class ArrayExtensionTests
    {
        [Test]
        public void RemoveAt1()
        {
            int[] arr = new int[] { 1, 2, 3 };
            Assert.AreEqual(new int[] { 1, 2 }, arr.RemoveAt(2));
        }

        [Test]
        public void RemoveAt2()
        {
            int[] arr = new int[] { 1, 2, 3 };
            Assert.AreEqual(new int[] { 2, 3 }, arr.RemoveAt(0));
        }

        [Test]
        public void RemoveAt3()
        {
            int[] arr = new int[] { 1, 2, 3 };
            Assert.AreEqual(new int[] { 1, 3 }, arr.RemoveAt(1));
        }

        [Test]
        public void RemoveAt4()
        {
            int[] arr = new int[] { 1, 2, 3 };
            Assert.Throws<IndexOutOfRangeException>(() => arr.RemoveAt(3));
        }
    }
}
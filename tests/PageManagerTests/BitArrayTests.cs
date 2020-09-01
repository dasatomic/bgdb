using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace PageManagerTests
{
    public unsafe class BitArrayTests
    {
        [Test]
        public void BitArrayIsSet()
        {
            byte[] data = new byte[] { 0, 1, 7, 42 };
            int[] checkPositions = new int[] { 8, 16, 17, 18, 25, 27, 29};

            fixed (byte* ptr = data)
            {
                for (int i = 0; i < 32; i++)
                {
                    if (checkPositions.Contains(i))
                    {
                        Assert.IsTrue(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                    else
                    {
                        Assert.IsFalse(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                }
            }
        }

        [Test]
        public void BitArraySet()
        {
            byte[] data = new byte[4];
            List<int> bytesSetPosition = new List<int>();

            Random rnd = new Random();
            fixed (byte* ptr = data)
            {
                for (int i = 0; i < 10; i++)
                {
                    int pos = rnd.Next(0, 32);
                    PageManager.UtilStructures.BitArray.Set(pos, ptr);
                    Assert.IsTrue(PageManager.UtilStructures.BitArray.IsSet(pos, ptr));
                    bytesSetPosition.Add(pos);
                }

                for (int i = 0; i < 32; i++)
                {
                    if (bytesSetPosition.Contains(i))
                    {
                        Assert.IsTrue(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                    else
                    {
                        Assert.IsFalse(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                }
            }
        }

        [Test]
        public void BitArrayUnset()
        {
            byte[] data = new byte[4] { byte.MaxValue, byte.MaxValue, byte.MaxValue, byte.MaxValue };
            List<int> bytesUnsetSetPosition = new List<int>();

            fixed (byte* ptr = data)
            {
                Random rnd = new Random();
                for (int i = 0; i < 10; i++)
                {
                    int pos = rnd.Next(0, 32);
                    PageManager.UtilStructures.BitArray.Unset(pos, ptr);
                    Assert.IsFalse(PageManager.UtilStructures.BitArray.IsSet(pos, ptr));
                    bytesUnsetSetPosition.Add(pos);
                }

                for (int i = 0; i < 32; i++)
                {
                    if (bytesUnsetSetPosition.Contains(i))
                    {
                        Assert.IsFalse(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                    else
                    {
                        Assert.IsTrue(PageManager.UtilStructures.BitArray.IsSet(i, ptr));
                    }
                }
            }
        }
    }
}

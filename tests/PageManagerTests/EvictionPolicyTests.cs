using NUnit.Framework;
using PageManager;
using PageManager.Exceptions;
using System.Collections.Generic;

namespace PageManagerTests
{
    public class EvictionPolicyTests
    {
        [Test]
        public void AddPagesNoEviction()
        {
            LruEvictionPolicy policy = new LruEvictionPolicy(10, 5);

            for (int i = 0; i < 10; i++)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            Assert.AreEqual(10, policy.CurrentPageCount());
            Assert.AreEqual(0, policy.FreePageCount());
        }

        [Test]
        public void AddPagesWithEviction()
        {
            LruEvictionPolicy policy = new LruEvictionPolicy(10, 5);

            for (int i = 0; i < 10; i++)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            Assert.AreEqual(new List<int> { 0, 1, 2, 3, 4 }, policy.RecordUsageAndEvict(11));
        }

        [Test]
        public void AddPagesWithEvictionNonSequentialAccess()
        {
            LruEvictionPolicy policy = new LruEvictionPolicy(10, 5);

            for (int i = 0; i < 10; i++)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            for (int i = 4; i >= 0; i--)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            Assert.AreEqual(new List<int> { 5, 6, 7, 8, 9 }, policy.RecordUsageAndEvict(11));
        }

        [Test]
        public void AddPagesWithEvictionAndAvoidPolicy()
        {
            LruEvictionPolicy policy = new LruEvictionPolicy(10, 5);

            for (int i = 0; i < 10; i++)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            Assert.AreEqual(new List<int> { 1, 2, 4, 5, 6 }, policy.RecordUsageAndEvict(11, new ulong[] { 0, 3 }));
        }

        [Test]
        public void AddPagesNoPoolSpace()
        {
            LruEvictionPolicy policy = new LruEvictionPolicy(10, 5);

            for (int i = 0; i < 10; i++)
            {
                Assert.IsEmpty(policy.RecordUsageAndEvict((ulong)i));
            }

            Assert.Throws<OutOfBufferPoolSpaceException>(() =>
            {
                policy.RecordUsageAndEvict(11, new ulong[] { 0, 1, 2, 3, 4, 5, 6 });
            });
        }
    }
}

using LockManager;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LockManagerTests
{
    public class LockManagerTests
    {
        [Test]
        public async Task LockAcquire()
        {
            ILockManager lckmgr = new LockManager.LockManager();
            using var lck = await lckmgr.AcquireLock(LockTypeEnum.Shared, 1, 1);
        }

        [Test]
        public async Task MultiLockAcquire()
        {
            ILockManager lckmgr = new LockManager.LockManager();

            for (int i = 0; i < 1000; i++)
            {
                using var lck = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, (ulong)i, 1);
            }
        }

        [Test]
        public async Task LockStressTest()
        {
            ILockManager lckmgr = new LockManager.LockManager();

            async Task acquireLockShared(int owner)
            {
                Random rnd = new Random();
                int id = rnd.Next(1, 1000);
                using var rel = await lckmgr.AcquireLock(LockTypeEnum.Shared, (ulong)id, (ulong)owner);
            }

            async Task acquireLockEx(int owner)
            {
                Random rnd = new Random();
                int id = rnd.Next(1, 1000);
                using var rel = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, (ulong)id, (ulong)owner);
            }

            List<Task> tasks = new List<Task>();

            const int taskCount = 10000;

            for (int i = 0; i < taskCount; i++)
            {
                if (i % 2 == 0)
                {
                    tasks.Add(acquireLockShared(i));
                }
                else
                {
                    tasks.Add(acquireLockEx(i));
                }
            }

            await Task.WhenAll(tasks);
        }
    }
}

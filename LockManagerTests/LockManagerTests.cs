using LockManager;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Text;
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
    }
}

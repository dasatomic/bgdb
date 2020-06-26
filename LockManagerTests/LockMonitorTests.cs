using LockManager;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LockManagerTests
{
    public class LockMonitorTests
    {
        [Test]
        public async Task LockMonitorAcquire()
        {
            LockMonitor lckMonitor = new LockMonitor();
            ILockManager lckmgr = new LockManager.LockManager(lckMonitor);
            using var lck = await lckmgr.AcquireLock(LockTypeEnum.Shared, 1, 1);
            using var lck1 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 2, 1);
            using var lck2 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 3, 1);

            using var lck3 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 3, 3);

            var lockSnapshot = lckMonitor.GetSnapshot();

            Assert.AreEqual(new ulong[] { 1, 3 }, lockSnapshot.Keys.OrderBy(x => x));

            Assert.AreEqual(new ulong[] { 1, 2, 3 }, lockSnapshot[1].Keys.OrderBy(x => x));
            Assert.AreEqual(new ulong[] { 3 }, lockSnapshot[3].Keys.OrderBy(x => x));
        }
    }
}

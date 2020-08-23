using LockManager;
using NUnit.Framework;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LockManagerTests
{
    public class LockMonitorTests
    {
        [Test]
        public async Task LockMonitorAcquire()
        {
            LockMonitor lckMonitor = new LockMonitor();
            ILockManager lckmgr = new LockManager.LockManager(lckMonitor, new NoOpLogging());
            using var lck = await lckmgr.AcquireLock(LockTypeEnum.Shared, 1, 1);
            using var lck1 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 2, 1);
            using var lck2 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 3, 1);

            using var lck3 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 3, 3);

            var lockSnapshot1 = lckMonitor.GetSnapshot(1);
            var lockSnapshot3 = lckMonitor.GetSnapshot(3);

            Assert.AreEqual(new ulong[] { 1, 2, 3 }, lockSnapshot1.Select(x => x.Item1).OrderBy(x => x));
            Assert.AreEqual(new ulong[] { 3 }, lockSnapshot3.Select(x => x.Item1).OrderBy(x => x));
        }

        [Test]
        public async Task DeadLockDetection()
        {
            LockMonitor lckMonitor = new LockMonitor();
            ILockManager lckmgr = new LockManager.LockManager(lckMonitor, new NoOpLogging());

            AutoResetEvent evt = new AutoResetEvent(false);

            Task lockAcquireTask = Task.Run(() =>
            {
                using var lck = lckmgr.AcquireLock(LockTypeEnum.Exclusive, 1, 1).Result;
                using var lck1 = lckmgr.AcquireLock(LockTypeEnum.Exclusive, 2, 2).Result;
                using var lck2 = lckmgr.AcquireLock(LockTypeEnum.Exclusive, 2, 1).Result;

                evt.WaitOne();
            });

            // Insure that deadlock monitor is in a right position
            //
            while (true)
            {
                await Task.Delay(100);
                var snapshot = lckMonitor.GetSnapshot(1);

                if (snapshot.Any(x => x.Item1 == 2))
                {
                    break;
                }
            }

            Assert.ThrowsAsync<DeadlockException>(async () =>
            {
                using var lck3 = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 1, 2);
            });

            evt.Set();
        }

        [Test]
        public async Task DeadLockDetectionCircle()
        {
            LockMonitor lckMonitor = new LockMonitor();
            ILockManager lckmgr = new LockManager.LockManager(lckMonitor, new NoOpLogging());
            AutoResetEvent evt1 = new AutoResetEvent(false);
            AutoResetEvent evt2 = new AutoResetEvent(false);
            AutoResetEvent evt3 = new AutoResetEvent(false);
            AutoResetEvent alldone = new AutoResetEvent(false);
            SemaphoreSlim sem = new SemaphoreSlim(0);

            Task la1 = Task.Run(async () =>
            {
                using var lck = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 1, 1);
                sem.Release();
                evt1.WaitOne();
                using var lc2 = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 2, 1);
                alldone.WaitOne();
            });

            Task la2 = Task.Run(async () =>
            {
                using var lck = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 2, 2);
                sem.Release();
                evt2.WaitOne();
                using var lck2 = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 3, 2);
                alldone.WaitOne();
            });

            Task la3 = Task.Run(async () =>
            {
                using var lck = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 3, 3);
                sem.Release();
                evt3.WaitOne();
            });

            // Get in position.
            await sem.WaitAsync();
            await sem.WaitAsync();
            await sem.WaitAsync();

            evt1.Set();
            evt2.Set();

            // Now if owner 3 asks for lck 1 we are in deadlock.
            // A->B->C -|
            // ^        |
            // |--------|
            Assert.ThrowsAsync<DeadlockException>(async () =>
            {
                using var lck = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 1, 3);
            });
        }

        [Test]
        public async Task ActiveLockSnapshotTest()
        {
            LockMonitor lckMonitor = new LockMonitor();
            ILockManager lckmgr = new LockManager.LockManager(lckMonitor, new NoOpLogging());

            var rel1 = await lckmgr.AcquireLock(LockTypeEnum.Shared, 1, 1);
            var rel2 = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 2, 2);
            var rel3 = await lckmgr.AcquireLock(LockTypeEnum.Exclusive, 3, 3);
            rel3.Dispose();
            Assert.AreEqual(
                new[] { 
                    new LockMonitorRecord(1, 1, LockTypeEnum.Shared),
                    new LockMonitorRecord(2, 2, LockTypeEnum.Exclusive) 
                },
                lckMonitor.GetActiveLocks());
        }
    }
}

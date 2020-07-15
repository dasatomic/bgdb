using System;
namespace LockManager
{
    public struct LockStatsRecord
    {
        public int LockId { get; private set; }
        public TimeSpan WaitDuration { get; private set; }
        public LockTypeEnum LockType { get; private set; }

        public LockStatsRecord(int lockId, TimeSpan waitDuration, LockTypeEnum lockType)
        {
            this.LockId = lockId;
            this.WaitDuration = waitDuration;
            this.LockType = lockType;
        }
    }

    public struct LockStats
    {
        public TimeSpan WaitTimePercentile50th;
        public TimeSpan WaitTimePercentile95th;
        public TimeSpan WaitTimePercentile99th;
        public TimeSpan WaitTimePercentileMax;

        public override string ToString()
        {
            return string.Format($"Lock stats: " +
                $"percentile 50th = {this.WaitTimePercentile50th.TotalMilliseconds}," +
                $"percentile 95th = {this.WaitTimePercentile95th}," +
                $"percentile 99th = {this.WaitTimePercentile99th}," +
                $"percentile 100th = {this.WaitTimePercentileMax}");
        }
    }
}

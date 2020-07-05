namespace LockManager
{
    public interface LockManagerInstrumentationInterface
    {
        void LogInfo(string message);
        void LogError(string message);
        void LogDebug(string message);
    }

    public class NoOpLogging : LockManagerInstrumentationInterface
    {
        public void LogDebug(string message)
        {
        }

        public void LogError(string message)
        {
        }

        public void LogInfo(string message)
        {
        }
    }
}

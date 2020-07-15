using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;

namespace Instrumentation
{
    public enum LogLevel
    {
        Debug = 0,
        Info = 1,
        Warning = 2,
        Error = 3,
    }

    public class Logger : PageManager.InstrumentationInterface, LockManager.LockManagerInstrumentationInterface
    {
        private readonly ILog eventLog;

        private static Level[] logLevelMapper = new Level[4]
        {
            Level.Debug,
            Level.Info,
            Level.Warn,
            Level.Error,
        };

        private Logger(string fileName, string repositoryName, Level logLevel)
        {
            Hierarchy hierarchy = (Hierarchy)log4net.LogManager.CreateRepository(repositoryName);

            PatternLayout patternLayout = new PatternLayout();
            patternLayout.ConversionPattern = "%date [%thread] %-5level %logger - %message%newline";
            patternLayout.ActivateOptions();

            RollingFileAppender roller = new RollingFileAppender();
            roller.AppendToFile = false;
            roller.File = fileName;
            roller.Layout = patternLayout;
            roller.MaxSizeRollBackups = 5;
            roller.MaximumFileSize = "1GB";
            roller.RollingStyle = RollingFileAppender.RollingMode.Size;
            roller.StaticLogFileName = true;
            roller.ActivateOptions();
            hierarchy.Root.AddAppender(roller);

            hierarchy.Root.Level = logLevel;
            hierarchy.Configured = true;


            this.eventLog = log4net.LogManager.GetLogger(repositoryName, "DefaultLogger");
        }

        public Logger(string fileName, string repositoryName, LogLevel logLevel)
            : this(fileName, repositoryName, logLevelMapper[(int)logLevel])
        {
        }

        public void LogInfo(string message)
        {
            this.eventLog.Info(message);
        }

        public void LogDebug(string message)
        {
            this.eventLog.Debug(message);
        }

        public void LogError(string message)
        {
            this.eventLog.Error(message);
        }
    }
}

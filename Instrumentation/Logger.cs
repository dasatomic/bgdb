using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository;
using log4net.Repository.Hierarchy;
using System;

namespace Instrumentation
{
    public class Logger : PageManager.InstrumentationInterface, LockManager.LockManagerInstrumentationInterface
    {
        private readonly ILog eventLog;

        public Logger(string fileName, string repositoryName)
        {
            Hierarchy hierarchy = (Hierarchy)log4net.LogManager.CreateRepository(repositoryName);

            PatternLayout patternLayout = new PatternLayout();
            patternLayout.ConversionPattern = "%date [%thread] %-5level %logger - %message%newline";
            patternLayout.ActivateOptions();

            RollingFileAppender roller = new RollingFileAppender();
            roller.AppendToFile = true;
            roller.File = fileName;
            roller.Layout = patternLayout;
            roller.MaxSizeRollBackups = 5;
            roller.MaximumFileSize = "1GB";
            roller.RollingStyle = RollingFileAppender.RollingMode.Size;
            roller.StaticLogFileName = true;
            roller.ActivateOptions();
            hierarchy.Root.AddAppender(roller);

            hierarchy.Root.Level = Level.Debug;
            hierarchy.Configured = true;


            this.eventLog = log4net.LogManager.GetLogger(repositoryName, "DefaultLogger");
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

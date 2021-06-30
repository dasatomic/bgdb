using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{
    public static class ProcessRunnerExtensions
    {
        /// <summary>
        /// Waits asynchronously for the process to exit.
        /// </summary>
        /// <param name="process">The process to wait for cancellation.</param>
        /// <param name="cancellationToken">A cancellation token. If invoked, the task will return 
        /// immediately as canceled.</param>
        /// <returns>A Task representing waiting for the process to end.</returns>
        public static Task WaitForExitAsync(this Process process,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (process.HasExited) return Task.CompletedTask;

            var tcs = new TaskCompletionSource<object>();
            process.EnableRaisingEvents = true;
            process.Exited += (sender, args) => tcs.TrySetResult(null);
            if (cancellationToken != default(CancellationToken))
                cancellationToken.Register(() => tcs.SetCanceled());

            return process.HasExited ? Task.CompletedTask : tcs.Task;
        }
    }

    public static class OperatingSystem
    {
        public static bool IsWindows() => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public static bool IsMacOS() => RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public static bool IsLinux() => RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

        public static string GetFfmpegExecPath()
        {
            if (IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                return Path.Combine(assemblyFolderPath, "ffmpeg");
            }
            else if (IsLinux() || IsMacOS())
            {
                return "/usr/bin/";
            }
            else
            {
                throw new NotImplementedException("Os currently not supported");
            }
        }

        public static string GetFfmpegPath()
        {
            if (IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                return Path.Combine(assemblyFolderPath, "ffmpeg/ffmpeg.exe");

            }
            else if (IsLinux() || IsMacOS())
            {
                // This means that ffmpeg needs to be installed.
                // TODO: Error handling if ffmpeg isn't installed/bash is not used etc.
                // For now this is only to pass basic test on Ubuntu when everything is correctly preinstalled.
                return "/usr/bin/ffmpeg";
            }
            else
            {
                throw new NotImplementedException("Os currently not supported");
            }
        }

        public static string GetFfProbePath()
        {
            if (IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                return Path.Combine(assemblyFolderPath, "ffmpeg/ffprobe.exe");

            }
            else if (IsLinux() || IsMacOS())
            {
                return "/usr/bin/ffprobe";
            }
            else
            {
                throw new NotImplementedException("Os currently not supported");
            }
        }
    }
}

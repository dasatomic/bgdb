using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{
    public class FfmpegVideoChunker
    {
        private const string ffArgsFormat = "-hide_banner -i {0} -c copy -map 0 -segment_time {1} -f segment {2}";
        private readonly string tempDestination;
        private readonly IVideoProcessingInstrumentationInterface logger;

        public FfmpegVideoChunker(string tempDestination, IVideoProcessingInstrumentationInterface logger)
        {
            this.tempDestination = tempDestination;
            this.logger = logger;
        }

        private ProcessStartInfo CreateProcessStartInfo(string ffmpegArgs)
        {
            ProcessStartInfo pci = new ProcessStartInfo();
            if (OperatingSystem.IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                pci.FileName = Path.Combine(assemblyFolderPath, "ffmpeg/ffmpeg.exe");
                pci.Arguments = ffmpegArgs;

            }
            else if (OperatingSystem.IsLinux())
            {
                // use bash + ffmpeg.
                // This means that ffmpeg needs to be installed.
                // TODO: Error handling if ffmpeg isn't installed/bash is not used etc.
                // For now this is only to pass basic test on Ubuntu when everything is correctly preinstalled.
                pci.FileName = "/bin/bash";
                pci.Arguments = $"-c \"ffmpeg {ffmpegArgs}\"";
            }
            else
            {
                throw new NotImplementedException("Os currently not supported");
            }

            pci.UseShellExecute = false;
            pci.RedirectStandardOutput = true;
            pci.RedirectStandardError = true;

            return pci;
        }

        public async Task<string[]> Execute(string fileName, TimeSpan span, CancellationToken token)
        {
            FileInfo fi = new FileInfo(fileName);
            string outputFileName = Path.GetFileNameWithoutExtension(fi.Name) + "%03d" + fi.Extension;

            string destinationDir = Path.Combine(tempDestination, Guid.NewGuid().ToString());
            Directory.CreateDirectory(destinationDir);
            string outputDestination = Path.Combine(destinationDir, outputFileName);

            string arguments = string.Format(ffArgsFormat, fileName, span, outputDestination);

            ProcessStartInfo pci = CreateProcessStartInfo(arguments);
            using (Process proc = Process.Start(pci))
            {
                this.logger.LogDebug($"Running Process name {proc.ProcessName} with id {proc.Id}.");
                await proc.WaitForExitAsync(token);
                this.logger.LogDebug($"Process Id {proc.Id} exited with exit code {proc.ExitCode}.");

                if (!proc.StandardOutput.EndOfStream)
                {
                    string output = await proc.StandardOutput.ReadToEndAsync();
                    output = $"Process id {proc.Id} standard output: " + Environment.NewLine + output;
                    this.logger.LogDebug(output);
                }

                if (!proc.StandardError.EndOfStream)
                {
                    string output = await proc.StandardError.ReadToEndAsync();
                    output = $"Process id {proc.Id} standard output: " + Environment.NewLine + output;
                    this.logger.LogDebug(output);
                }
            }

            return Directory.GetFiles(destinationDir);
        }
    }
}

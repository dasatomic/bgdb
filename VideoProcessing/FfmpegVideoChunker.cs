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

        public FfmpegVideoChunker(string tempDestination)
        {
            this.tempDestination = tempDestination;
        }

        private ProcessStartInfo CreateProcessStartInfo(string ffmpegArgs)
        {
            if (OperatingSystem.IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                string exePath = Path.Combine(assemblyFolderPath, "ffmpeg/ffmpeg.exe");
                ProcessStartInfo pci = new ProcessStartInfo(exePath, ffmpegArgs);

                pci.UseShellExecute = false;
                pci.RedirectStandardOutput = true;
                pci.RedirectStandardError = true;

                return pci;
            }
            else if (OperatingSystem.IsLinux())
            {
                // use bash + ffmpeg.
                // This means that ffmpeg needs to be installed.
                // TODO: Error handling if ffmpeg isn't installed/bash is not used etc.
                // For now this is only to pass basic test on Ubuntu when everything is correctly preinstalled.
                ProcessStartInfo pci = new ProcessStartInfo("/bin/bash", $"-c \"ffmpeg {ffmpegArgs}\"");

                pci.UseShellExecute = false;
                pci.RedirectStandardOutput = true;
                pci.RedirectStandardError = true;

                return pci;
            }

            throw new NotImplementedException("Os currently not supported");
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
                await proc.WaitForExitAsync(token);

                if (!proc.StandardOutput.EndOfStream)
                {
                    // TODO: Log
                    string jsonOutput = await proc.StandardOutput.ReadToEndAsync();
                }

                if (!proc.StandardError.EndOfStream)
                {
                    // TODO: Log
                    string jsonOutput = await proc.StandardError.ReadToEndAsync();
                }
            }

            return Directory.GetFiles(destinationDir);
        }
    }
}

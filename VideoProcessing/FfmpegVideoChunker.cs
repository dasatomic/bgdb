using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{
    public class FfmpegVideoChunker
    {
        private static string GetFfmpegPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "ffmpeg/ffmpeg.exe");
        }

        // %03d for format.
        private const string ffArgsFormat = "-hide_banner -i {0} -c copy -map 0 -segment_time {1} -f segment {2}";
        private readonly string ffmpegExePath;
        private readonly string tempDestination;

        public FfmpegVideoChunker(string tempDestination)
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new NotImplementedException("Currently ffmpeg run is only supported for Windows");
            }

            this.ffmpegExePath = Path.GetFullPath(GetFfmpegPath());
            this.tempDestination = tempDestination;
        }

        public async Task<string[]> Execute(string fileName, TimeSpan span, CancellationToken token)
        {
            FileInfo fi = new FileInfo(fileName);
            string outputFileName = Path.GetFileNameWithoutExtension(fi.Name) + "%03d" + fi.Extension;

            string destinationDir = Path.Combine(tempDestination, Guid.NewGuid().ToString());
            Directory.CreateDirectory(destinationDir);
            string outputDestination = Path.Combine(destinationDir, outputFileName);

            string arguments = string.Format(ffArgsFormat, fileName, span, outputDestination);

            ProcessStartInfo pci = new ProcessStartInfo(this.ffmpegExePath, arguments);
            pci.UseShellExecute = false;
            pci.RedirectStandardOutput = true;
            pci.RedirectStandardError = true;

            Process proc = Process.Start(pci);
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

            return Directory.GetFiles(destinationDir);
        }
    }
}

using System;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;
using System.IO;
using Newtonsoft.Json;

namespace VideoProcessing
{
    public class FfmpegProbeWrapper
    {
        private static string GetFfmpegPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "ffmpeg/ffprobe.exe");
        }

        private readonly string ffmpegProbeExePath;

        private const string ffProbeArgs = " -v quiet -print_format json -show_format -show_streams -hide_banner ";

        public FfmpegProbeWrapper()
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new NotImplementedException("Currently ffmpeg run is only supported for Windows");
            }

            this.ffmpegProbeExePath = Path.GetFullPath(GetFfmpegPath());
        }

        public async Task<FfProbeOutputSerializer> Execute(string videoName, CancellationToken cancellationToken)
        {
            string args = ffProbeArgs + videoName;
            ProcessStartInfo pci = new ProcessStartInfo(ffmpegProbeExePath, args);
            pci.UseShellExecute = false;
            pci.RedirectStandardOutput = true;
            pci.RedirectStandardError = true;

            Process proc = Process.Start(pci);
            await proc.WaitForExitAsync(cancellationToken);

            if (!proc.StandardOutput.EndOfStream)
            {
                string jsonOutput = await proc.StandardOutput.ReadToEndAsync();

                return JsonConvert.DeserializeObject<FfProbeOutputSerializer>(jsonOutput);
            }

            throw new FfProbeErrorOutputException("Invalid input");
        }
    }
}

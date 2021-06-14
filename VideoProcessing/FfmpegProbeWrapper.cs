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
        private const string ffProbeArgs = " -v quiet -print_format json -show_format -show_streams -hide_banner ";

        public FfmpegProbeWrapper() { }

        private ProcessStartInfo CreateProcessStartInfo(string ffProbeArgs)
        {
            if (OperatingSystem.IsWindows())
            {
                FileInfo dataRoot = new FileInfo(typeof(FfmpegProbeWrapper).Assembly.Location);
                string assemblyFolderPath = dataRoot.Directory.FullName;
                string exePath = Path.Combine(assemblyFolderPath, "ffmpeg/ffprobe.exe");
                ProcessStartInfo pci = new ProcessStartInfo(exePath, ffProbeArgs);

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
                ProcessStartInfo pci = new ProcessStartInfo("/bin/bash", $"-c \"ffprobe {ffProbeArgs}\"");

                pci.UseShellExecute = false;
                pci.RedirectStandardOutput = true;
                pci.RedirectStandardError = true;

                return pci;
            }

            throw new NotImplementedException("Os currently not supported");
        }

        public async Task<FfProbeOutputSerializer> Execute(string videoName, CancellationToken cancellationToken)
        {
            string args = ffProbeArgs + videoName;
            ProcessStartInfo pci = CreateProcessStartInfo(args);

            using (Process proc = Process.Start(pci))
            {
                await proc.WaitForExitAsync(cancellationToken);

                if (!proc.StandardOutput.EndOfStream)
                {
                    string jsonOutput = await proc.StandardOutput.ReadToEndAsync();

                    return JsonConvert.DeserializeObject<FfProbeOutputSerializer>(jsonOutput);
                }
            }

            throw new FfProbeErrorOutputException("Invalid input");
        }
    }
}

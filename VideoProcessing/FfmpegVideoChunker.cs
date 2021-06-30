using PageManager;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xabe.FFmpeg;

namespace VideoProcessing
{
    public class FfmpegVideoChunker
    {
        private readonly string tempDestination;
        private readonly IVideoProcessingInstrumentationInterface logger;

        public FfmpegVideoChunker(string tempDestination, IVideoProcessingInstrumentationInterface logger)
        {
            this.tempDestination = tempDestination;
            this.logger = logger;
        }

        public async Task<string[]> Execute(string fileName, TimeSpan span, ITransaction tran, CancellationToken token)
        {
            FileInfo fi = new FileInfo(fileName);
            string outputFileName = Path.GetFileNameWithoutExtension(fi.Name) + "%03d" + fi.Extension;

            string destinationDir = Path.Combine(tempDestination, Guid.NewGuid().ToString());
            DirectoryInfo tempDir = Directory.CreateDirectory(destinationDir);
            tran.RegisterTempFolder(tempDir);
            string outputDestination = Path.Combine(destinationDir, outputFileName);

            FFmpeg.SetExecutablesPath(OperatingSystem.GetFfmpegExecPath());
            var conversionResult = await FFmpeg.Conversions.New()
                .AddParameter($"-i {fileName}")
                .AddParameter($"-c copy")
                .AddParameter("-map 0")
                .AddParameter($"-segment_time {span.TotalSeconds.ToString()}")
                .AddParameter("-f segment")
                .SetOutput(outputDestination)
                .Start();

            this.logger.LogDebug($"Video chunker took {conversionResult.Duration.TotalSeconds}s");

            return Directory.GetFiles(destinationDir);
        }
    }
}

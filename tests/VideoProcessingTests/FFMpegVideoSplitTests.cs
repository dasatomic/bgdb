using NUnit.Framework;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using VideoProcessing;

namespace VideoProcessingTests
{
    public class FFMpegVideoSplitTests
    {
        private static string GetExampleVideoPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(FFmpegProbeTests).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "examples/sample_960x400_ocean_with_audio.mkv");
        }

        private static string GetTempFolderPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(FFmpegProbeTests).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;

            string path = Path.Combine(assemblyFolderPath, "temp");
            Directory.CreateDirectory(path);
            return path;
        }

        [Test]
        public async Task FFmpegVideoChunkerTests()
        {
            var videoChunker = new FfmpegVideoChunker(GetTempFolderPath());

            string[] chunkPaths = await videoChunker.Execute(GetExampleVideoPath(), TimeSpan.FromSeconds(10), CancellationToken.None);

            Assert.AreEqual(5, chunkPaths.Length);
        }
    }
}

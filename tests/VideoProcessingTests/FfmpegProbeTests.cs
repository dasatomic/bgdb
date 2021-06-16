using NUnit.Framework;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using VideoProcessing;

namespace VideoProcessingTests
{
    public class FFmpegProbeTests
    {
        private static string GetExampleVideoPath()
        {
            FileInfo dataRoot = new FileInfo(typeof(FFmpegProbeTests).Assembly.Location);
            string assemblyFolderPath = dataRoot.Directory.FullName;
            return Path.Combine(assemblyFolderPath, "examples/sample_960x400_ocean_with_audio.mkv");
        }

        [Test]
        public async Task FFmpegProbeOutputGeneration()
        {
            var ffmpegProbeWrapper = new FfmpegProbeWrapper(new NoOpLogging());

            var output = await ffmpegProbeWrapper.Execute(GetExampleVideoPath(), CancellationToken.None);
            Assert.AreEqual(2, output.Streams.Length);
            Assert.AreEqual("video", output.Streams[0].CodecType);
            Assert.AreEqual("h264", output.Streams[0].CodeName);
            Assert.AreEqual(400, output.Streams[0].Height);
            Assert.AreEqual(960, output.Streams[0].Width);

            Assert.AreEqual("audio", output.Streams[1].CodecType);
            Assert.AreEqual(2, output.Streams[1].Channels);
            Assert.AreEqual("vorbis", output.Streams[1].CodeName);
            Assert.AreEqual(48000, output.Streams[1].SampleRate);

            Assert.AreEqual(2976636, output.Format.BitRate);
            Assert.AreEqual(46.616, output.Format.DurationInSeconds);
            Assert.AreEqual("matroska,webm", output.Format.FormatName);
        }
    }
}
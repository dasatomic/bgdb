using PageManager;
using System;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public static class SourceProvidersSignatures
    {
        public struct VideoChunkerResult
        {
            public string ChunkPath;
            public int NbStreams;
            public int NbPrograms;
            public double StartTimeInSeconds;
            public double DurationInSeconds;
            public string FormatName;
            public int BitRate;
        }

        public struct ExtractedImageResult
        {
            public string Path;
        }

        public delegate Task<VideoChunkerResult[]> VideoChunkerProvider(string videoPath, TimeSpan timespan, ITransaction tran);

        public delegate Task<ExtractedImageResult[]> VideoToImageProvider(string videoPath, int framesPerDuration, int durationInSeconds, ITransaction tran);
    }
}

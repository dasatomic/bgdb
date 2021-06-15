using PageManager;
using System;
using System.Threading;
using static QueryProcessing.SourceProvidersSignatures;

namespace VideoProcessing
{
    public static class SourceRegistration
    {
        public static VideoChunkerProvider VideoChunkerCallback(FfmpegVideoChunker videoChunker, FfmpegProbeWrapper videoProbe)
        {
            return async (string path, TimeSpan chunk, ITransaction tran) =>
            {
                string[] chunkPaths = await videoChunker.Execute(path, chunk, tran, CancellationToken.None);

                VideoChunkerResult[] chunkerResult = new VideoChunkerResult[chunkPaths.Length];

                int i = 0;
                foreach (string videoChunk in chunkPaths)
                {
                    FfProbeOutputSerializer probeOutput = await videoProbe.Execute(videoChunk, CancellationToken.None);
                    chunkerResult[i].ChunkPath = videoChunk;
                    chunkerResult[i].NbStreams= probeOutput.Format.NbStreams;
                    chunkerResult[i].NbPrograms= probeOutput.Format.NbPrograms;
                    chunkerResult[i].StartTimeInSeconds = probeOutput.Format.StartTimeInSeconds;
                    chunkerResult[i].DurationInSeconds = probeOutput.Format.DurationInSeconds;
                    chunkerResult[i].FormatName = probeOutput.Format.FormatName;
                    chunkerResult[i].BitRate = probeOutput.Format.BitRate;

                    i++;
                }

                return chunkerResult;
            };
        }

        public static VideoToImageProvider VideoToImageCallback(FfmpegFrameExtractor frameExtractor)
        {
            return async (string path, int framesPerDuration, int durationInSeconds, ITransaction tran) =>
            {
                string[] chunkPaths = await frameExtractor.Execute(path, framesPerDuration, durationInSeconds, tran, CancellationToken.None);
                ExtractedImageResult[] extractorResults = new ExtractedImageResult[chunkPaths.Length];

                int i = 0;
                foreach (string imagePath in chunkPaths)
                {
                    extractorResults[i].Path = imagePath;
                    i++;
                }

                return extractorResults;
            };
        }
    }
}

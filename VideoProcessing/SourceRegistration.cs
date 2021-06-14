using PageManager;
using QueryProcessing;
using System;
using System.Threading;

namespace VideoProcessing
{
    public static class SourceRegistration
    {
        public static SourceProvidersSignatures.VideoChunkerProvider VideoChunkerCallback(FfmpegVideoChunker videoChunker)
        {
            return async (string path, TimeSpan chunk, ITransaction tran) =>
            {
                string[] chunkPaths = await videoChunker.Execute(path, chunk, tran, CancellationToken.None);

                return new SourceProvidersSignatures.VideoChunkerResult
                {
                    ChunkPaths = chunkPaths,
                };
            };
        }
    }
}

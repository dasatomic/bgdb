using PageManager;
using System;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public static class SourceProvidersSignatures
    {
        public struct VideoChunkerResult
        {
            public string[] ChunkPaths;
        }

        public delegate Task<VideoChunkerResult> VideoChunkerProvider(string videoPath, TimeSpan timespan, ITransaction tran);
    }
}

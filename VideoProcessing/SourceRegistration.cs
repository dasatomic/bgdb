using System;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{
    public static class SourceRegistration
    {
        public static Func<string, TimeSpan, Task<string[]>> VideoChunkerCallback(FfmpegVideoChunker videoChunker)
        {
            return async (string path, TimeSpan chunk) =>
            {
                string[] chunkPaths = await videoChunker.Execute(path, chunk, CancellationToken.None);
                return chunkPaths;
            };
        }
    }
}

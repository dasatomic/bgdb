using PageManager;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace VideoProcessing
{
    public static class SourceRegistration
    {
        public static Func<string, TimeSpan, ITransaction, Task<string[]>> VideoChunkerCallback(FfmpegVideoChunker videoChunker)
        {
            return async (string path, TimeSpan chunk, ITransaction tran) =>
            {
                string[] chunkPaths = await videoChunker.Execute(path, chunk, tran, CancellationToken.None);
                return chunkPaths;
            };
        }
    }
}

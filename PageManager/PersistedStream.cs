using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IPersistedStream : IDisposable
    {
        public string GetFileName();
        public ulong CurrentFileSize();
        public void Grow(ulong newSize);
        public void Shrink(ulong newSize);
        public Task SeekAndWrite(ulong position, IPage page);
        public Task<IPage> SeekAndRead(ulong position, PageType pageType, IBufferPool bufferPool, ColumnInfo[] columnInfos);
        public bool IsInitialized();
        public void MarkInitialized();
        public Task<IPage> GetShadowPage(PageType pageType, int pageSize, ColumnInfo[] columnInfos);
    }

    public class PersistedStream : IPersistedStream
    {
        private string fileName;

        private FileStream fileStream;
        private BinaryWriter binaryWriter;
        private BinaryReader binaryReader;
        private bool isInitialized;
        private SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        InstrumentationInterface logger = null;

        /// <summary>
        /// Shadow page stream is used to insure page validity on the flush and insure
        /// that engine is able to recover in case of interrupt during the flush.
        /// Protocol:
        /// On page flush:
        /// 1) Push to shadowPageStream.
        /// 2) Push to persistedStream.
        /// On recovery:
        /// 1) Check if page is present in shadowPageStream.
        /// 2) If it is present, data in is should be valid. In case of restart in the middle of flush page in persisted stream might be garbage.
        /// 3) If it is not present, that means that page in persisted stream is valid since it already went through
        ///    shadowPage => persistedPage => (someone else overwrote shadowPage).
        /// </summary>
        private FileStream fileStreamShadow;
        private BinaryWriter binaryWriterShadow;
        private BinaryReader binaryReaderShadow;

        public PersistedStream(ulong startFileSize, string fileName, bool createNew)
            : this(startFileSize, fileName, createNew, new NoOpLogging()) { }

        public PersistedStream(ulong startFileSize, string fileName, bool createNew, InstrumentationInterface logger)
        {
            this.logger = logger;

            string shadowFileName = fileName + ".shadow";

            if (File.Exists(fileName) && !createNew)
            {
                this.fileStream = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite);
                this.fileStreamShadow = new FileStream(shadowFileName, FileMode.Open, FileAccess.ReadWrite);
                isInitialized = true;
            }
            else
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                    File.Delete(shadowFileName);
                }

                this.fileStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite);
                this.fileStream.SetLength((long)startFileSize);
                this.fileStreamShadow = new FileStream(shadowFileName, FileMode.CreateNew, FileAccess.ReadWrite);
                isInitialized = false;
            }

            this.binaryWriter = new BinaryWriter(this.fileStream);
            this.binaryReader = new BinaryReader(this.fileStream);
            this.binaryReaderShadow = new BinaryReader(this.fileStreamShadow);
            this.binaryWriterShadow = new BinaryWriter(this.fileStreamShadow);

            this.fileName = fileName;
        }

        public ulong CurrentFileSize() => (ulong)this.fileStream.Length;

        public string GetFileName() => this.fileName;

        public void Grow(ulong newSize)
        {
            if ((ulong)this.fileStream.Length > newSize)
            {
                throw new ArgumentException();
            }

            this.fileStream.SetLength((long)newSize);
        }

        public void Shrink(ulong newSize)
        {
            if ((ulong)this.fileStream.Length < newSize)
            {
                throw new ArgumentException();
            }

            this.fileStream.SetLength((long)newSize);
        }

        public async Task SeekAndWrite(ulong position, IPage page)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                // First push to the shadow stream to insure validity in case of failure.
                this.fileStreamShadow.Seek(0, SeekOrigin.Begin);
                page.Persist(this.binaryWriterShadow);
                this.binaryWriterShadow.Flush();
                await this.fileStreamShadow.FlushAsync().ConfigureAwait(false);

                this.fileStream.Seek((long)position, SeekOrigin.Begin);
                page.Persist(this.binaryWriter);

                this.binaryWriter.Flush();

                await this.fileStream.FlushAsync().ConfigureAwait(false);
                this.logger.LogDebug($"Flushed at location {position} to disk.");
            }
            finally
            {
                semaphore.Release();
            }
        }

        public async Task<IPage> SeekAndRead(ulong position, PageType pageType, IBufferPool bufferPool, ColumnInfo[] columnInfos)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                this.fileStream.Seek((long)position, SeekOrigin.Begin);

                (Memory<byte> memory, ulong token) = bufferPool.GetMemory();

                return pageType switch
                {
                    PageType.MixedPage => new MixedPage(this.binaryReader, memory, token, columnInfos),
                    PageType.StringPage => new StringOnlyPage(this.binaryReader),
                    _ => throw new ArgumentException()
                };
            }
            finally
            {
                semaphore.Release();
                this.logger.LogDebug($"Read from location {position} in buffer pool.");
            }
        }

        public void Dispose()
        {
            this.fileStream.Dispose();
            this.fileStreamShadow.Dispose();
            this.binaryReader.Dispose();
            this.binaryReaderShadow.Dispose();
            this.binaryWriter.Dispose();
            this.binaryWriterShadow.Dispose();
        }

        public bool IsInitialized() => this.isInitialized;

        public void MarkInitialized()
        {
            this.isInitialized = true;
        }

        public async Task<IPage> GetShadowPage(PageType pageType, int pageSize, ColumnInfo[] columnInfos)
        {
            // No real need to lock here since this will be fetched only during recovery.
            // But do it for safety...
            await semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                this.fileStreamShadow.Seek(0, SeekOrigin.Begin);
                // Use heap for shadow page. No need to go through buffer pool.
                const ulong dummyBufferPoolToken = ulong.MaxValue;
                Memory<byte> memory = new Memory<byte>(new byte[pageSize - IPage.FirstElementPosition]);

                return pageType switch
                {
                    PageType.MixedPage => new MixedPage(this.binaryReaderShadow, memory, token: dummyBufferPoolToken, columnInfos: columnInfos),
                    PageType.StringPage => new StringOnlyPage(this.binaryReaderShadow),
                    _ => throw new ArgumentException()
                };
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}

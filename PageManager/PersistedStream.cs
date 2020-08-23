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
        public Task<IPage> SeekAndRead(ulong position, PageType pageType, ColumnType[] columnTypes);
        public bool IsInitialized();
        public void MarkInitialized();
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

        public PersistedStream(ulong startFileSize, string fileName, bool createNew)
            : this(startFileSize, fileName, createNew, new NoOpLogging()) { }

        public PersistedStream(ulong startFileSize, string fileName, bool createNew, InstrumentationInterface logger)
        {
            this.logger = logger;

            if (File.Exists(fileName) && !createNew)
            {
                this.fileStream = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite);
                isInitialized = true;
            }
            else
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                }

                this.fileStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.ReadWrite);
                this.fileStream.SetLength((long)startFileSize);
                isInitialized = false;
            }

            this.binaryWriter = new BinaryWriter(this.fileStream);
            this.binaryReader = new BinaryReader(this.fileStream);

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

        public async Task<IPage> SeekAndRead(ulong position, PageType pageType, ColumnType[] columnTypes)
        {
            await semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                this.fileStream.Seek((long)position, SeekOrigin.Begin);

                return pageType switch
                {
                    PageType.DoublePage => new DoubleOnlyPage(this.binaryReader),
                    PageType.IntPage => new IntegerOnlyPage(this.binaryReader),
                    PageType.LongPage => new LongOnlyPage(this.binaryReader),
                    PageType.MixedPage => new MixedPage(this.binaryReader, columnTypes),
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
            this.binaryReader.Dispose();
        }

        public bool IsInitialized() => this.isInitialized;

        public void MarkInitialized()
        {
            this.isInitialized = true;
        }
    }
}

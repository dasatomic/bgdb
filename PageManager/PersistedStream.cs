using System;
using System.IO;

namespace PageManager
{
    public interface IPersistedStream : IDisposable
    {
        public string GetFileName();
        public ulong CurrentFileSize();
        public void Grow(ulong newSize);
        public void Shrink(ulong newSize);
        public void SeekAndWrite(ulong position, Action<BinaryWriter> writer);
        public IPage SeekAndRead(ulong position, Func<BinaryReader, IPage> reader);
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

        public PersistedStream(ulong startFileSize, string fileName, bool createNew)
        {
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

        public void SeekAndWrite(ulong position, Action<BinaryWriter> writer)
        {
            this.fileStream.Seek((long)position, SeekOrigin.Begin);
            writer(this.binaryWriter);
            this.fileStream.Flush();
        }

        public IPage SeekAndRead(ulong position, Func<BinaryReader, IPage> reader)
        {
            this.fileStream.Seek((long)position, SeekOrigin.Begin);
            return reader(this.binaryReader);
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

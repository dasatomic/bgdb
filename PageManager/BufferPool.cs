using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace PageManager
{
    public interface IBufferPool
    {
        public IPage GetPage(ulong id);
        public void AddPage(IPage page);
        public void EvictPage(ulong id, ulong token);
        public int PagesInPool();
        public IEnumerable<IPage> GetAllDirtyPages();

        /// <summary>
        /// Allocates memory for new page.
        /// </summary>
        /// <returns>Returns chunk of memory, if available, plus token used to return the memory.</returns>
        public (Memory<byte> memory, ulong token) GetMemory();
    }

    public class BufferPool : IBufferPool
    {
        private ConcurrentDictionary<ulong, IPage> pageCollection = new ConcurrentDictionary<ulong, IPage>();
        private Memory<byte> bufferPoolMemory;
        private readonly HashSet<ulong> takenChunks = new HashSet<ulong>();
        private readonly HashSet<ulong> freeChunks = new HashSet<ulong>();
        private int pageSize;
        private object lck = new object();

        public BufferPool(uint bufferPoolSizeMb, int pageSize)
        {
            ulong bufferPoolSize = bufferPoolSizeMb * 1024 * 1024;
            
            if (bufferPoolSize % (uint)pageSize != 0)
            {
                throw new ArgumentException();
            }

            // Buffer pool only tracks portion that is not sitting in the page.
            this.pageSize = pageSize - (int)IPage.FirstElementPosition;

            this.bufferPoolMemory = new byte[bufferPoolSize];
            for (ulong i = 0; i < bufferPoolSize; i += (uint)this.pageSize)
            {
                this.freeChunks.Add(i);
            }
        }

        public (Memory<byte> memory, ulong token) GetMemory()
        {
            lock (this.lck)
            {
                ulong chunk = this.freeChunks.First();
                this.freeChunks.Remove(chunk);
                this.takenChunks.Add(chunk);
                return (this.bufferPoolMemory.Slice((int)chunk, this.pageSize), chunk);
            }
        }

        public void AddPage(IPage page)
        {
            // TODO: For now even failure to add is fine.
            // It is possible that during fetch two concurrent readers try to fetch the same page
            // and both of them will try to update the BP.
            // It is Ok if only one of the succeeds.
            this.pageCollection.TryAdd(page.PageId(), page);
        }

        public void EvictPage(ulong id, ulong token)
        {
            pageCollection.Remove(id, out IPage _);

            lock (this.lck)
            {
                this.takenChunks.Remove(token);
                this.freeChunks.Add(token);
            }
        }

        public IEnumerable<IPage> GetAllDirtyPages()
        {
            foreach (IPage page in pageCollection.Values.Where(p => p.IsDirty()))
            {
                yield return page;
            }
        }

        public IPage GetPage(ulong id)
        {
            return pageCollection.GetValueOrDefault(id);
        }

        public int PagesInPool() => this.pageCollection.Count;
    }
}

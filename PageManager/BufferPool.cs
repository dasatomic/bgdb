using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace PageManager
{
    public interface IBufferPool
    {
        public IPage GetPage(ulong id);
        public void AddPage(IPage page);
        public void EvictPage(ulong id);
        public int PagesInPool();
        public IEnumerable<IPage> GetAllDirtyPages();
    }

    public class BufferPool : IBufferPool
    {
        private ConcurrentDictionary<ulong, IPage> pageCollection = new ConcurrentDictionary<ulong, IPage>();

        public void AddPage(IPage page)
        {
            // TODO: For now even failure to add is fine.
            // It is possible that during fetch two concurrent readers try to fetch the same page
            // and both of them will try to update the BP.
            // It is Ok if only one of the succeeds.
            pageCollection.TryAdd(page.PageId(), page);
        }

        public void EvictPage(ulong id)
        {
            pageCollection.Remove(id, out IPage _);
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

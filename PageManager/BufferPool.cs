using System.Collections.Generic;

namespace PageManager
{
    public interface IBufferPool
    {
        public IPage GetPage(ulong id);
        public void AddPage(IPage page);
        public void EvictPage(ulong id);
        public int PagesInPool();
    }

    public class BufferPool : IBufferPool
    {
        private Dictionary<ulong, IPage> pageCollection = new Dictionary<ulong, IPage>();

        public void AddPage(IPage page)
        {
            pageCollection.Add(page.PageId(), page);
        }

        public void EvictPage(ulong id)
        {
            pageCollection.Remove(id);
        }

        public IPage GetPage(ulong id)
        {
            return pageCollection.GetValueOrDefault(id);
        }

        public int PagesInPool() => this.pageCollection.Count;
    }
}

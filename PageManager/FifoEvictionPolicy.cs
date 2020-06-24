using System.Collections.Generic;

namespace PageManager
{
    public class FifoEvictionPolicy : IPageEvictionPolicy
    {
        private LinkedList<ulong> pages = new LinkedList<ulong>();
        private readonly ulong pageCountLimit;
        private readonly int evictCountOnReachingLimit;
        private object lck = new object();

        public FifoEvictionPolicy(ulong pageCountLimit, int evictCountOnReachingLimit)
            => (this.pageCountLimit, this.evictCountOnReachingLimit) = (pageCountLimit, evictCountOnReachingLimit);

        public ulong CurrentPageCount() => (ulong)pages.Count;

        public ulong FreePageCount() => this.pageCountLimit - (ulong)pages.Count;

        public ulong InMemoryPageCountLimit() => this.pageCountLimit;

        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId)
        {
            lock (lck)
            {
                var node = pages.Find(pageId);

                if (node == null)
                {
                    pages.AddFirst(pageId);
                }
                else
                {
                    pages.Remove(node);
                    pages.AddFirst(pageId);
                }

                List<ulong> pagesToRemove = new List<ulong>();
                if (pages.Count > (int)pageCountLimit)
                {
                    for (int i = 0; i < evictCountOnReachingLimit; i++)
                    {
                        pagesToRemove.Add(pages.Last.Value);
                        pages.RemoveLast();
                    }
                }

                return pagesToRemove;
            }
        }
    }
}

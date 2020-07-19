using PageManager.Exceptions;
using System.Collections.Generic;
using System.Linq;

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

        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId) => this.RecordUsageAndEvict(pageId, Enumerable.Empty<ulong>());

        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId, IEnumerable<ulong> pagesToAvoid)
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
                    if (pages.Count - pagesToAvoid.Count() < this.evictCountOnReachingLimit)
                    {
                        throw new OutOfBufferPoolSpaceException();
                    }

                    int i = 0;
                    LinkedListNode<ulong> lastUsedPage = pages.Last;
                    while (i < evictCountOnReachingLimit)
                    {
                        if (pagesToAvoid.Contains(lastUsedPage.Value))
                        {
                            lastUsedPage = lastUsedPage.Previous;
                        }
                        else
                        {
                            pagesToRemove.Add(lastUsedPage.Value);
                            LinkedListNode<ulong> tmpToRemove = lastUsedPage;
                            lastUsedPage = lastUsedPage.Previous;
                            pages.Remove(tmpToRemove);
                            i++;
                        }
                    }
                }

                return pagesToRemove;
            }
        }
    }
}

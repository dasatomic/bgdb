using System.Collections.Generic;

namespace PageManager
{
    public interface IPageEvictionPolicy
    {
        public ulong InMemoryPageCountLimit();
        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId);
        public ulong CurrentPageCount();
        public ulong FreePageCount();
    }
}

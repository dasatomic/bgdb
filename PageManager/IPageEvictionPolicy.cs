using System.Collections.Generic;

namespace PageManager
{
    interface IPageEvictionPolicy
    {
        public ulong InMemoryPageCountLimit();
        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId);
        public ulong CurrentPageCount();
        public ulong FreePageCount();
    }
}

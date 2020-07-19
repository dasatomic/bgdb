using System.Collections.Generic;

namespace PageManager
{
    public interface IPageEvictionPolicy
    {
        public ulong InMemoryPageCountLimit();
        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId);
        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId, IEnumerable<ulong> pagesToAvoid);
        public ulong CurrentPageCount();
        public ulong FreePageCount();
    }
}

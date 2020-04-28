using System;
using System.Collections.Generic;

namespace PageManager
{
    public class InMemoryPageManager : IPageManager
    {
        public IPage AllocatePage()
        {
            throw new NotImplementedException();
        }

        public IPage GetPage(long pageId)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IPage> GetPages()
        {
            throw new NotImplementedException();
        }

        public void SavePage(IPage page)
        {
            throw new NotImplementedException();
        }
    }
}

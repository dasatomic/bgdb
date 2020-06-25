using PageManager;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{
    public interface ILogManager
    {
        public Task CommitTransaction(ITransaction tran);
        public Task Flush();
        Task Recovery(BinaryReader reader, IPageManager pageManager, ITransaction tran);
        public ITransaction CreateTransaction(IPageManager manager, bool isReadOnly, string name);
        public ITransaction CreateTransaction(IPageManager manager, string name);
        public ITransaction CreateTransaction(IPageManager manager);
    }
}
